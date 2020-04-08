package com.navercorp.nbasearc.gcp;

import static org.junit.Assert.*;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class RedisDecoderTest {

	RedisDecoder decoder = new RedisDecoder();
	Charset utf8 = Charset.forName("UTF-8");
	
	@Test
	public void parseError() {
		ByteBuf in = Unpooled.buffer();
		List<byte[]> out = new ArrayList<byte[]>();

		in.writeBytes("-ERR".getBytes(utf8));
		decoder.getFrames(in, out);
		assertEquals(0, out.size());
		
		in.writeBytes("\r\n".getBytes(utf8));
		decoder.getFrames(in, out);
		assertEquals(1, out.size());
		assertArrayEquals("-ERR\r\n".getBytes(utf8), out.get(0));
	}

}
