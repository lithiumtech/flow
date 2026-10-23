/*
 * Copyright 2026 Lithium Technologies, Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.google.common.primitives.Longs;

public class MeasureTest {
	private static final long DAY = 86_400_000L;

	@Test
	public void binsStayBoundedOverLongUptime() throws Exception {
		Measure measure = new Measure("events", String::valueOf);
		measure.addDone(1);

		// a long-running process: the first value was recorded 30 days ago
		Field startTime = Measure.class.getDeclaredField("startTime");
		startTime.setAccessible(true);
		startTime.setLong(measure, System.currentTimeMillis() - 30 * DAY);
		measure.addDone(1);

		Field bins = Measure.class.getDeclaredField("bins");
		bins.setAccessible(true);
		int length = ((long[]) bins.get(measure)).length;
		assertTrue("bins.length=" + length, length <= 8192);
		assertEquals(2, measure.getDone());
	}

	@Test
	public void avgMatchesUnboundedBinsOverHoursWithIdleGaps() {
		AtomicLong clock = new AtomicLong(1_700_000_000_000L);
		Measure measure = new Measure("m", String::valueOf, clock::get);
		LegacyMeasure legacy = new LegacyMeasure(clock::get);
		Random random = new Random(25926);

		long end = clock.get() + 3 * 3600_000L;
		while (clock.get() < end) {
			// mostly busy seconds, with idle gaps of up to 25 minutes (longer than the 1024 bin padding)
			clock.addAndGet(random.nextInt(10) == 0 ? random.nextInt(1_500_000) : random.nextInt(2_000));
			long value = random.nextInt(100);
			measure.addDone(value);
			legacy.addDone(value);

			for (long interval : new long[] {1_000, 60_000, 300_000, Measure.HISTORY * 1000L, 10 * DAY}) {
				assertEquals("interval=" + interval, legacy.avg(interval), measure.avg(interval), 0);
			}
		}
		assertEquals(legacy.done, measure.getDone());
		assertTrue("old bins were dropped", firstBin(measure) > 0);
	}

	@Test
	public void wholeRunAvgMatchesUnboundedBinsOverTenDays() {
		AtomicLong clock = new AtomicLong(1_700_000_000_000L);
		Measure measure = new Measure("m", String::valueOf, clock::get);
		LegacyMeasure legacy = new LegacyMeasure(clock::get);
		Random random = new Random(563);

		long start = clock.get();
		long end = start + 10 * DAY;
		while (clock.get() < end) {
			clock.addAndGet(1 + random.nextInt(2_000_000));
			long value = random.nextInt(1000);
			measure.addDone(value);
			legacy.addDone(value);

			long elapsed = clock.get() - start;
			assertEquals(legacy.avg(elapsed), measure.avg(elapsed), 0);
			assertEquals(legacy.avg(60_000), measure.avg(60_000), 0);
		}
	}

	private static int firstBin(Measure measure) {
		try {
			Field field = Measure.class.getDeclaredField("firstBin");
			field.setAccessible(true);
			return field.getInt(measure);
		} catch (ReflectiveOperationException e) {
			throw new AssertionError(e);
		}
	}

	/** The implementation before bins were bounded, kept here as the reference for avg(). */
	private static class LegacyMeasure {
		private final java.util.function.LongSupplier clock;
		private long[] bins = new long[1024];
		private long startTime;
		private long done;

		LegacyMeasure(java.util.function.LongSupplier clock) {
			this.clock = clock;
		}

		void addDone(long value) {
			if (startTime == 0) {
				startTime = clock.getAsLong();
			}
			int bin = getBin(clock.getAsLong());
			bins = Longs.ensureCapacity(bins, bin + 1, 1024);
			bins[bin] += value;
			done += value;
		}

		double avg(long interval) {
			if (startTime == 0) {
				return 0;
			}
			long time = clock.getAsLong();
			int startBin = Math.max(0, getBin(time - interval));
			int endBin = getBin(time);
			if (startBin == endBin) {
				return 0;
			}
			double total = 0;
			double count = 0;
			for (int i = startBin; i < Math.min(endBin, bins.length - 1); i++) {
				total += bins[i];
				count++;
			}
			return total / count;
		}

		private int getBin(long time) {
			return (int) ((time - startTime) / 1000);
		}
	}
}
