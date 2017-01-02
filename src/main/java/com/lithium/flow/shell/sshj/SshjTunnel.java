/*
 * Copyright 2015 Lithium Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lithium.flow.shell.sshj;

import static com.google.common.base.Preconditions.checkNotNull;

import com.lithium.flow.shell.Tunnel;
import com.lithium.flow.shell.Tunneling;
import com.lithium.flow.util.Logs;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;

import net.schmizz.concurrent.Event;
import net.schmizz.sshj.common.LoggerFactory;
import net.schmizz.sshj.common.SSHPacket;
import net.schmizz.sshj.common.StreamCopier;
import net.schmizz.sshj.connection.Connection;
import net.schmizz.sshj.connection.channel.SocketStreamCopyMonitor;
import net.schmizz.sshj.connection.channel.direct.AbstractDirectChannel;

/**
 * @author Matt Ayres
 */
public class SshjTunnel extends Thread implements Tunnel {
	private static final Logger log = Logs.getLogger();

	private final Tunneling tunneling;
	private final ServerSocket server;
	private final Connection connection;
	private final AtomicBoolean closed = new AtomicBoolean();

	public SshjTunnel(@Nonnull Sshj ssh, @Nonnull Tunneling tunneling) throws IOException {
		checkNotNull(ssh);
		this.tunneling = checkNotNull(tunneling);

		server = buildServerSocket(ssh.getRetries());
		server.bind(new InetSocketAddress(getHost(), tunneling.getListen()));

		connection = ssh.getConnection();

		start();
	}

	@Override
	public void run() {
		log.debug("listening on port {}", server.getLocalSocketAddress());
		while (!closed.get()) {
			Socket socket = null;
			try {
				socket = server.accept();
				accept(socket);
			} catch (Exception e) {
				log.debug("failed to accept connection", e);
				IOUtils.closeQuietly(socket);
			}
		}
		log.debug("tunnel closed");
	}

	private void accept(@Nonnull Socket socket) throws IOException {
		log.debug("connection from {}", socket.getRemoteSocketAddress());

		AbstractDirectChannel channel = new AbstractDirectChannel(connection, "direct-tcpip") {
			@Override
			@Nonnull
			protected SSHPacket buildOpenReq() {
				return super.buildOpenReq()
						.putString(tunneling.getHost())
						.putUInt32(tunneling.getPort())
						.putString(getHost())
						.putUInt32(server.getLocalPort());
			}
		};
		channel.open();

		socket.setSendBufferSize(channel.getLocalMaxPacketSize());
		socket.setReceiveBufferSize(channel.getRemoteMaxPacketSize());
		Event<IOException> soc2chan = new StreamCopier(socket.getInputStream(), channel.getOutputStream(),
				LoggerFactory.DEFAULT).bufSize(channel.getRemoteMaxPacketSize()).spawnDaemon("soc2chan");
		Event<IOException> chan2soc = new StreamCopier(channel.getInputStream(), socket.getOutputStream(),
				LoggerFactory.DEFAULT).bufSize(channel.getLocalMaxPacketSize()).spawnDaemon("chan2soc");
		SocketStreamCopyMonitor.monitor(5, TimeUnit.SECONDS, soc2chan, chan2soc, channel, socket);
	}

	@Override
	@Nonnull
	public String getHost() {
		return "localhost";
	}

	@Override
	public int getPort() {
		return server.getLocalPort();
	}

	@Override
	public void close() throws IOException {
		closed.set(true);
		server.close();
	}

	@Nonnull
	private ServerSocket buildServerSocket(int tries) throws IOException {
		int count = 0;
		while (true) {
			try {
				return new ServerSocket();
			} catch (BindException e) {
				if (++count == tries) {
					throw e;
				}
			}
		}
	}
}
