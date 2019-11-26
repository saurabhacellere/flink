/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.runtime.rest.messages.ErrorResponseBody;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.slf4j.Logger;

/**
 * This is the last handler in the pipeline. It logs all error messages.
 */
@ChannelHandler.Sharable
public class PipelineErrorHandler extends SimpleChannelInboundHandler<HttpRequest> {

	/** The logger to which the handler writes the log statements. */
	private final Logger logger;

	public PipelineErrorHandler(Logger logger) {
		this.logger = logger;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, HttpRequest message) {
		// we can't deal with this message. No one in the pipeline handled it. Log it.
		logger.warn("Unknown message received: {}", message);
		AbstractRestHandler.sendErrorResponse(new ErrorResponseBody("Bad request received."), HttpResponseStatus.BAD_REQUEST, ctx, message);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		logger.warn("Unhandled exception", cause);
	}
}
