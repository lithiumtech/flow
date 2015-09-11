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

package com.lithium.flow.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;

/**
 * @author Matt Ayres
 */
public class LoopThread extends Thread {
	private static final Logger log = Logs.getLogger();
	private static final AtomicInteger nextId = new AtomicInteger();

	private final long interval;
	private final Executable executable;
	private final Consumer<Exception> onException;
	private long nextTime;

	public LoopThread(@Nonnull Executable executable) {
		this(0, executable);
	}

	public LoopThread(long interval, @Nonnull Executable executable) {
		this(interval, 0, false, executable);
	}

	public LoopThread(long interval, long offset, boolean round, @Nonnull Executable executable) {
		this(null, interval, offset, round, executable, exception -> {
			log.warn("loop iteration failed", exception);
		});
	}

	public LoopThread(@Nullable String name, long interval, long offset, boolean round,
			@Nonnull Executable executable, @Nonnull Consumer<Exception> onException) {
		super(name != null ? name : "LoopThread-" + nextId.incrementAndGet());
		this.interval = interval;
		this.executable = checkNotNull(executable);
		this.onException = checkNotNull(onException);

		long time = System.currentTimeMillis();
		nextTime = time + offset - (round && interval > 0 ? time % interval : 0);

		setDaemon(true);
		start();
	}

	@Override
	public void run() {
		while (!interrupted()) {
			try {
				executable.call();
			} catch (Exception e) {
				onException.accept(e);
			}

			if (interval > 0) {
				nextTime += interval;

				long time = System.currentTimeMillis();
				if (time > nextTime) {
					while (time > nextTime) {
						nextTime += interval;
					}
					nextTime -= interval;
				}

				if (!Sleep.until(nextTime)) {
					break;
				}
			}
		}
	}

	@Nonnull
	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {
		private String name;
		private long interval;
		private long offset;
		private boolean round;
		private Executable executable;
		private Consumer<Exception> onException;

		@Nonnull
		public Builder setName(@Nonnull String name) {
			this.name = checkNotNull(name);
			return this;
		}

		@Nonnull
		public Builder interval(long interval) {
			this.interval = interval;
			return this;
		}

		@Nonnull
		public Builder offset(long offset) {
			this.offset = offset;
			return this;
		}

		@Nonnull
		public Builder round(boolean round) {
			this.round = round;
			return this;
		}

		@Nonnull
		public Builder execute(@Nonnull Executable executable) {
			this.executable = checkNotNull(executable);
			return this;
		}

		@Nonnull
		public Builder onException(@Nonnull Consumer<Exception> onException) {
			this.onException = checkNotNull(onException);
			return this;
		}

		@Nonnull
		public LoopThread build() {
			return new LoopThread(name, interval, offset, round, executable, onException);
		}
	}
}
