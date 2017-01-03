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

import com.lithium.flow.io.InputStreamSpliterator;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import com.google.common.base.Throwables;

/**
 * @author Matt Ayres
 */
public class Lines {
	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

	@Nonnull
	public static Stream<String> stream(@Nonnull InputStream in) throws IOException {
		checkNotNull(in);
		return stream(in, DEFAULT_CHARSET);
	}

	@Nonnull
	public static Stream<String> stream(@Nonnull InputStream in, @Nonnull Charset charset) throws IOException {
		checkNotNull(in);
		checkNotNull(charset);
		return StreamSupport.stream(new InputStreamSpliterator(in, charset), false);
	}

	public static void accept(@Nonnull InputStream in,
			@Nonnull CheckedConsumer<String, IOException> consumer) throws IOException {
		checkNotNull(in);
		checkNotNull(consumer);
		accept(in, DEFAULT_CHARSET, consumer);
	}

	public static void accept(@Nonnull InputStream in, @Nonnull Charset charset,
			@Nonnull CheckedConsumer<String, IOException> consumer) throws IOException {
		checkNotNull(in);
		checkNotNull(charset);
		checkNotNull(consumer);

		LineIterator it = IOUtils.lineIterator(in, charset);
		try {
			while (it.hasNext()) {
				consumer.accept(it.nextLine());
			}
		} catch (IllegalStateException e) {
			Throwables.throwIfInstanceOf(e.getCause(), IOException.class);
			throw new IOException(e);
		} finally {
			it.close();
		}
	}

	@Nonnull
	public static Iterable<String> iterate(@Nonnull InputStream in) throws IOException {
		checkNotNull(in);
		return iterate(in, DEFAULT_CHARSET);
	}

	@Nonnull
	public static Iterable<String> iterate(@Nonnull InputStream in, @Nonnull Charset charset) throws IOException {
		checkNotNull(in);
		checkNotNull(charset);

		LineIterator it = IOUtils.lineIterator(in, charset);
		AtomicBoolean used = new AtomicBoolean();

		return () -> {
			if (used.getAndSet(true)) {
				throw new RuntimeException("this iterable can only be used once");
			}

			return new Iterator<String>() {
				@Override
				public boolean hasNext() {
					boolean next = it.hasNext();
					if (!next) {
						it.close();
					}
					return next;
				}

				@Override
				public String next() {
					return it.nextLine();
				}
			};
		};
	}
}
