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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author Matt Ayres
 */
public class LoopQueue<T> {
	private final int capacity;
	private final BlockingQueue<T> queue;
	private final List<LoopThread> threads = new CopyOnWriteArrayList<>();
	private volatile boolean finish;

	/**
	 * @deprecated use {@link #LoopQueue()} and {@link #forList(CheckedConsumer)} instead.
	 */
	@Deprecated
	public LoopQueue(@Nonnull Consumer<List<T>> consumer) {
		this(Integer.MAX_VALUE);
		forList(consumer::accept);
	}

	/**
	 * @deprecated use {@link #LoopQueue(int)} and {@link #forList(CheckedConsumer)} instead.
	 */
	@Deprecated
	public LoopQueue(int capacity, @Nonnull Consumer<List<T>> consumer) {
		this(capacity);
		forList(consumer::accept);
	}

	public LoopQueue() {
		this(Integer.MAX_VALUE);
	}

	public LoopQueue(int capacity) {
		this(capacity, (Comparator<T>) null);
	}

	public LoopQueue(int capacity, @Nullable Comparator<T> comparator) {
		this.capacity = capacity;
		queue = comparator == null
				? new LinkedBlockingQueue<T>(capacity)
				: new PriorityBlockingQueue<>(capacity, comparator);
	}

	public void forList(@Nonnull CheckedConsumer<List<T>, Exception> consumer) {
		forList(consumer, () -> !queue.isEmpty(), Integer.MAX_VALUE);
	}

	public void forList(int batch, @Nonnull CheckedConsumer<List<T>, Exception> consumer) {
		forList(consumer, () -> !queue.isEmpty() && (queue.size() >= batch || finish), batch);
	}

	private void forList(@Nonnull CheckedConsumer<List<T>, Exception> consumer, @Nonnull Checker checker, int max) {
		threads.add(new LoopThread(10, () -> {
			while (queue.size() > 0 && checker.check()) {
				List<T> list = new ArrayList<>();
				queue.drainTo(list, max);
				if (list.size() > 0) {
					consumer.accept(list);
				}
			}
		}));
	}

	public void forEach(@Nonnull CheckedConsumer<T, Exception> consumer) {
		forList(list -> {
			for (T item : list) {
				consumer.accept(item);
			}
		});
	}

	public void add(@Nonnull T element) {
		checkNotNull(element);
		Sleep.until(() -> queue.size() < capacity && queue.offer(element));
	}

	public int size() {
		return queue.size();
	}

	public void finish() {
		finish = true;
		Sleep.until(queue::isEmpty);
		threads.forEach(LoopThread::finish);
	}
}
