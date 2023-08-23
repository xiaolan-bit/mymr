package com.ksc.wordcount.shuffle.nettyimpl.server;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.ksc.wordcount.conf.AppConfig;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.shuffle.nettyimpl.FileComplate;
import com.ksc.wordcount.task.KeyValue;
import io.netty.channel.*;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.shuffle.nettyimpl.FileComplate;
import io.netty.channel.*;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;

public class ShuffleServiceHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        System.out.println("ShuffleServiceHandler received:" + msg);
        if (msg instanceof ShuffleBlockId) {
            ShuffleBlockId shuffleBlockId = (ShuffleBlockId) msg;
            System.out.println("ShuffleServiceHandler received:" + ((ShuffleBlockId) msg).name());
            File file = new File(shuffleBlockId.getShufflePath());
            if (file.exists()) {
                try (Input input = new Input(new FileInputStream(file))) {
                    Kryo kryo = new Kryo(); // Create a Kryo instance and register KeyValue.class
                    kryo.register(KeyValue.class);

                    Object obj;
                    while ((obj = kryo.readObject(input, KeyValue.class)) != null) {
                        System.out.println("Read object: " + obj); // Add this line for debugging
                        ctx.writeAndFlush(obj);
                    }
                    System.out.println("Finished reading data.");
                }

                System.out.println("ShuffleServiceHandler send FileComplate");
                ctx.writeAndFlush(new FileComplate());
            } else {
                ctx.writeAndFlush("shuffle File not found: " + file.getAbsolutePath());
            }
        } else {
            super.channelRead(ctx, msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
