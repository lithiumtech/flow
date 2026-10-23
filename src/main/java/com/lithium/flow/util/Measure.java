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

import java.util.function.Function;
import java.util.function.LongSupplier;

import javax.annotation.Nonnull;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

/**
 * @author Matt Ayres
 */
public class Measure {
	/** Seconds of per-second history that {@link #avg(long)} can see, beyond the start of the measure. */
	static final int HISTORY = 3600;
	private static final int PADDING = 1024;
	private static final int MAX_BINS = HISTORY + PADDING + 1;

	private final String name;
	private final Function<Number, String> printer;
	private final LongSupplier clock;
	private final Multiset<Object> uniques = HashMultiset.create();
	private long[] bins = new long[PADDING];
	/** Bin number held in {@code bins[0]}. Bins before it are dropped and only their sum is kept. */
	private int firstBin;
	/** Sum of the values of the bins before {@link #firstBin}. */
	private long pastDone;
	private long startTime;
	private long todo;
	private long done;
	private boolean forEta;
	private boolean hideAvg;

	public Measure(@Nonnull String name, @Nonnull Function<Number, String> printer) {
		this(name, printer, System::currentTimeMillis);
	}

	Measure(@Nonnull String name, @Nonnull Function<Number, String> printer, @Nonnull LongSupplier clock) {
		this.name = checkNotNull(name);
		this.printer = checkNotNull(printer);
		this.clock = checkNotNull(clock);
	}

	public boolean isForEta() {
		return forEta;
	}

	@Nonnull
	public Measure useForEta() {
		forEta = true;
		return this;
	}

	@Nonnull
	public Measure hideAvg() {
		hideAvg = true;
		return this;
	}

	public synchronized void add(@Nonnull Object object) {
		checkNotNull(object);
		uniques.add(object);
		done = uniques.elementSet().size();
	}

	public synchronized void remove(@Nonnull Object object) {
		checkNotNull(object);
		uniques.remove(object);
		done = uniques.elementSet().size();
	}

	public boolean isHideAvg() {
		return hideAvg;
	}

	@Nonnull
	public String getName() {
		return name;
	}

	@Nonnull
	public Function<Number, String> getPrinter() {
		return printer;
	}

	public void incTodo() {
		addTodo(1);
	}

	public void incSkip() {
		addSkip(1);
	}

	public void decTodo() {
		addTodo(-1);
	}

	public void subTodo(long value) {
		addTodo(-value);
	}

	public synchronized void addTodo(long value) {
		todo += value;
	}

	public void incDone() {
		addDone(1);
	}

	public void decDone() {
		addDone(-1);
	}

	public void subDone(long value) {
		addDone(-value);
	}

	public synchronized void addDone(long value) {
		long time = clock.getAsLong();
		if (startTime == 0) {
			startTime = time;
		}

		int bin = getBin(time);
		if (bin >= firstBin + bins.length) {
			// grow as before, to this bin plus padding, but keep at most MAX_BINS bins:
			// one bin per second since the start would otherwise grow without bound
			int endBin = bin + 1 + PADDING;
			int newFirstBin = Math.max(firstBin, endBin - MAX_BINS);
			int keep = Math.max(0, firstBin + bins.length - newFirstBin);
			for (int i = 0; i < bins.length - keep; i++) {
				pastDone += bins[i];
			}

			long[] newBins = new long[endBin - newFirstBin];
			System.arraycopy(bins, bins.length - keep, newBins, 0, keep);
			bins = newBins;
			firstBin = newFirstBin;
		}

		if (bin >= 0 && bin < firstBin) {
			pastDone += value;
		} else {
			bins[bin - firstBin] += value;
		}
		done += value;
	}

	public synchronized void addSkip(long value) {
		todo += value;
		done += value;
	}

	public synchronized long getTodo() {
		return todo;
	}

	public synchronized long getDone() {
		return done;
	}

	public synchronized long getLeft() {
		return todo - done;
	}

	/**
	 * Average per second over the whole seconds of the last {@code interval}. Exact for an interval of up to
	 * {@link #HISTORY} or one that reaches back to the start of the measure; any other longer interval is
	 * averaged over the last {@link #HISTORY} seconds of it.
	 */
	public synchronized double avg(long interval) {
		if (startTime == 0) {
			return 0;
		}

		long time = clock.getAsLong();
		int startBin = Math.max(0, getBin(time - interval));
		int endBin = getBin(time);
		if (startBin == endBin) {
			return 0;
		}

		int stopBin = Math.min(endBin, firstBin + bins.length - 1);
		double total = 0;
		double count = 0;
		if (startBin == 0 && firstBin > 0) {
			total += pastDone;
			count += Math.min(firstBin, stopBin);
		}
		for (int i = Math.max(startBin, firstBin); i < stopBin; i++) {
			total += bins[i - firstBin];
			count++;
		}
		return total / count;
	}

	private int getBin(long time) {
		return (int) ((time - startTime) / 1000);
	}
}
