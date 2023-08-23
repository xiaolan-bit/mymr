package com.ksc.urltopn.thrift.server;

import com.ksc.urltopn.thrift.UrlTopNService;
//import com.ksc.wordcount.driver.service.ServiceImpl;
import com.ksc.wordcount.driver.service.ServiceImpl;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;

public class ThriftServer {
    public static void main(String[] args) {
        try {
            // 创建Service的处理器，并关联到ServiceImpl的实现。
            UrlTopNService.Processor<ServiceImpl> processor = new UrlTopNService.Processor<>(new ServiceImpl());
            // 使用TServerSocket进行TCP传输，设置服务端口为9091
            TServerSocket serverSocket = new TServerSocket(9091);
            // 使用特定的序列化协议
            TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
            // 创建并启动Thrift服务器
            TServer server = new TSimpleServer(new TServer.Args(serverSocket).processor(processor).protocolFactory(protocolFactory));
            System.out.println("Server started on port 9091...");
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
