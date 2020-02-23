import json
from io import BytesIO

from avro.io import DatumWriter, BinaryEncoder, DatumReader, BinaryDecoder
from avro.schema import SchemaFromJSONData, Names
from gevent import socket, monkey
from gevent.queue import Queue
from loguru import logger

monkey.patch_all()

queue = Queue(100)


class Meta:
    def __init__(self, callback, service_name, param_schema, result_schema, version=0):
        self.callback = callback
        self.service_name = service_name
        self.param_schema = SchemaFromJSONData(param_schema, Names())
        self.result_schema = SchemaFromJSONData(result_schema, Names())
        self.version = version
        self._param_writer = DatumWriter(self.param_schema)
        self._param_reader = DatumReader(self.param_schema)
        self._result_writer = DatumWriter(self.result_schema)
        self._result_reader = DatumReader(self.result_schema)

    def decode_param(self, byte_mem):
        return self._param_reader.read(BinaryDecoder(BytesIO(byte_mem)))

    def encode_param(self, param):
        logger.info(param)
        io = BytesIO()
        self._param_writer.write(param, BinaryEncoder(io))
        return io.getbuffer().tobytes()

    def decode_result(self, byte_mem):
        return self._result_reader.read(BinaryDecoder(BytesIO(byte_mem)))

    def encode_result(self, result):
        io = BytesIO()
        self._result_writer.write(result, BinaryEncoder(io))
        return io.getbuffer().tobytes()


# [1:serviceLen][0-238:serviceName][4:requestId][4:clientId][4:serverId][1:bodyType][4:bodyLen][0-4G:data]

class RequestMessage:
    def __init__(self, service_len):
        self.service_len = service_len
        self.service_name = None
        self.request_id = None
        self.client_id = None
        self.server_id = None
        self.body_type = None
        self.body_len = None
        self.body = None

    def header_readed(self):
        return self.body_len is not None

    def msg_len(self):
        return self.body_len + self.service_len + 18


class Server:
    def __init__(self, host, port):
        self._sock = None
        self._host = host
        self._port = port
        self._request_close = False
        self._services = {}

    def add_service(self, meta):
        if meta.service_name in self._services.keys():
            raise RuntimeError("%s service registered" % meta.service_name)
        self._services[meta.service_name] = meta

    def start(self):
        if len(self._services) == 0:
            raise RuntimeError("no service registered")
        if self._sock:
            raise RuntimeError("server started")

        left_bytes = self._shake_hands()
        self._do_service(left_bytes)

    def _shake_hands(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((self._host, self._port))
        self._sock.sendall(self._encode_shake_req())
        resp_bytes, shake_pkg_len = self._read_shake_resp()
        left_bytes = self._decode_shake_resp(resp_bytes, shake_pkg_len)
        return left_bytes

    def _call_service(self, req_msg):
        """
        //服务器报错 40 - 60
        // 服务器收到不正确的消息 40 - 49
        public static final int CODE_SERVER_DECODE_ERROR = 40;
        public static final int CODE_SERVER_PARAM_VALID_ERROR = 41;
        public static final int CODE_SERVER_NO_SERVICE = 42;
        // 服务器运行时错误
        // 同步
        public static final int CODE_SERVER_RUNTIME_ERROR = 50;
        // 异步
        public static final int CODE_SERVER_ASYNC_RUNTIME_ERROR = 51;
        :param req_msg: RequestMsg
        :return: Response
        """
        meta = self._services.get(req_msg.service_name)

        ret_code = 0
        err_str = None
        if not meta:
            err_str = "no service found"
            ret_code = 42

        if req_msg.body_len > 0:
            try:
                param = meta.decode_param(req_msg.body)
            except Exception as e:
                err_str = str(e)
                logger.error(e)
                ret_code = 40
        else:
            param = None

        resp_buf = None
        if ret_code == 0:
            logger.info("start invoke %s" % req_msg.service_name)
            try:
                result = meta.callback(param)
                if result is not None:
                    resp_buf = meta.encode_result(result)
            except Exception as e:
                ret_code = 50
                resp_buf = str(e)
        else:
            resp_buf = err_str.encode()
        self._encode_and_send_response(req_msg, ret_code, resp_buf)

    def _encode_and_send_response(self, req_msg, ret_code, resp_buf):
        """
        PACK_STRUCT_START ResponseId {
        uint8_t headerLen;
        uint32_t requestId;
        uint32_t clientId;
        uint32_t serverId;
        uint8_t responseCode;
        uint8_t bodyType;
        uint32_t bodyLen;
        char data[];
        }PACK_STRUCT_END
        :param req_msg: request message
        :param ret_code: code
        :param resp_buf: body bytearray
        :return:
        """
        body_len = 0 if not resp_buf else len(resp_buf)
        ret = bytearray(19 + body_len)
        ret[0] = 18  # 固定18
        ret[1:5] = req_msg.request_id.to_bytes(4, 'little')
        ret[5:9] = req_msg.client_id.to_bytes(4, 'little')
        ret[9:13] = req_msg.server_id.to_bytes(4, 'little')
        ret[13] = ret_code
        ret[14] = 0  # body type 0
        ret[15:19] = body_len.to_bytes(4, 'little')
        if body_len > 0:
            ret[19:] = resp_buf
        self._sock.sendall(ret)

    def _do_service(self, left_bytes):
        logger.info("server started, do service...")
        if left_bytes:
            req_msg, left_bytes = self._decode_req_msg(left_bytes)
        else:
            req_msg = None

        while not self._request_close:
            if not left_bytes:
                left_bytes = self._sock.recv(65536)
            elif isinstance(left_bytes, bytearray):
                left_bytes[len(left_bytes):] = self._sock.recv(65536)
            else:
                left_bytes = bytearray(left_bytes)
                left_bytes[len(left_bytes):] = self._sock.recv(65536)
            if not left_bytes:
                self._sock.close()
                return
            req_msg, left_bytes = self._decode_req_msg(left_bytes, req_msg)

    def _decode_req_msg(self, left_bytes, req_msg=None):
        pck_offset = 0
        while pck_offset < len(left_bytes):
            if not req_msg:
                req_msg = RequestMessage(left_bytes[pck_offset])
            if not req_msg.header_readed():
                if len(left_bytes) >= pck_offset + req_msg.service_len + 18:
                    # 足够解析header
                    req_id_offset = pck_offset + 1 + req_msg.service_len
                    req_msg.service_name = left_bytes[pck_offset + 1:req_id_offset].decode()
                    req_msg.request_id = int.from_bytes(left_bytes[req_id_offset:req_id_offset + 4], 'little')
                    req_msg.client_id = int.from_bytes(left_bytes[req_id_offset + 4:req_id_offset + 8], 'little')
                    req_msg.server_id = int.from_bytes(left_bytes[req_id_offset + 8:req_id_offset + 12], 'little')
                    req_msg.body_type = left_bytes[req_id_offset + 12]
                    req_msg.body_len = int.from_bytes(left_bytes[req_id_offset + 13:req_id_offset + 17], 'little')
                else:
                    return req_msg, left_bytes[pck_offset:]

            # header readed
            if req_msg.msg_len() + pck_offset > len(left_bytes):
                # body没有发送完毕
                return req_msg, left_bytes[pck_offset:]
            if req_msg.body_len > 0:
                req_msg.body = left_bytes[pck_offset + req_msg.service_len + 18: pck_offset + req_msg.msg_len()]
            self._call_service(req_msg)
            pck_offset += req_msg.msg_len()
            req_msg = None
        return None, None

    def _decode_shake_resp(self, resp_bytes, shake_pkg_len):
        decode_len = 6
        for i in range(len(self._services)):
            if decode_len + 1 > shake_pkg_len:
                self._sock.close()
                raise RuntimeError("invalid shake response")

            service_len = resp_bytes[decode_len]
            decode_len = decode_len + 1

            if decode_len + service_len > shake_pkg_len:
                self._sock.close()
                raise RuntimeError("invalid shake response")
            service_name = resp_bytes[decode_len:decode_len + service_len].decode()
            if service_name not in self._services.keys():
                self._sock.close()
                raise RuntimeError("not registered service %s" % service_name)
            decode_len = decode_len + service_len

            if decode_len + 1 > shake_pkg_len:
                self._sock.close()
                raise RuntimeError("invalid shake response")
            service_count = resp_bytes[decode_len]
            decode_len = decode_len + 1
            logger.info("service %s registered success, provide count %d" % (service_name, service_count))

        if decode_len < len(resp_bytes):
            return resp_bytes[decode_len:]
        else:
            return None

    def _read_shake_resp(self):
        service_size = len(self._services)
        shake_resp = bytearray()
        shake_pkg_len = 0
        shake_resp_len = 0
        while len(shake_resp) < 4 or shake_resp_len < len(shake_resp):
            recv = self._sock.recv(1024)
            if not recv:
                self._sock.close()
                raise RuntimeError("connection closed")
            shake_resp[shake_resp_len:] = recv
            shake_resp_len = len(shake_resp)
            if not shake_pkg_len and shake_resp_len >= 6:
                shake_pkg_len = int.from_bytes(shake_resp[0:4], "little") + 4
                recv_service_size = int.from_bytes(shake_resp[4:6], "little")
                if recv_service_size != service_size:
                    self._sock.close()
                    raise RuntimeError("shake error because send %d receive %d" % (service_size, recv_service_size))
        return shake_resp, shake_pkg_len

    def _encode_shake_req(self):
        b = bytearray(7)
        data_len = 3
        b[4] = 255
        b[5:7] = int.to_bytes(len(self._services), 2, "little")
        for meta in self._services.values():
            service_name = meta.service_name
            param = json.dumps(meta.param_schema.to_json())
            result = json.dumps(meta.param_schema.to_json())
            serv_len = len(service_name)
            b.append(serv_len)
            b[len(b):] = service_name.encode()
            # param
            data_len = data_len + serv_len + 1
            req_len = len(param)
            b[len(b):] = req_len.to_bytes(4, "little")
            b[len(b):] = param.encode()
            data_len = data_len + req_len + 4
            # result
            resp_len = len(result)
            b[len(b):] = resp_len.to_bytes(4, "little")
            b[len(b):] = result.encode()
            data_len = data_len + resp_len + 4
        b[0:4] = data_len.to_bytes(4, "little")
        return b


param_schema_dict = {"type": "array", "items": {"type": "record", "name": "User",
                                                "namespace": "com.zhw.skynet.service",
                                                "fields": [{"name": "name", "type": "string"},
                                                           {"name": "age", "type": "int"},
                                                           {"name": "sex", "type": "boolean"}]},
                     "java-class": "java.util.List"}
result_schema_dict = "string"


def user_tell_desc(users):
    return "收到user%d个" % len(users)


if __name__ == '__main__':
    server = Server("ubuntu", 9998)
    tell_desc_meta = Meta(user_tell_desc, 'com.zhw.skynet.service.UserService/tellDesc',
                          param_schema_dict, result_schema_dict)
    server.add_service(tell_desc_meta)
    server.start()
