package com.lithium.flow.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.lithium.flow.util.Batcher.Batch;

/**
 * Created by bdharrington7 on 10/26/17.
 */
public class BatcherTest {

	@Test
	public void testCreateBatch() {
		Batcher<Integer> batcher = new Batcher<>(3);
		ArrayList<Integer> items = Lists.newArrayList(1, 2, 3);
		for (Integer item : items) {
			batcher.offer(item);
		}

		try {
			Batch<Integer> taken = batcher.take();
			assertNotNull(taken);

			int idx = 0;
			for (int i : taken) {
				assertEquals(i, (int)items.get(idx++));
			}

		} catch (InterruptedException iex) {
			fail("Exception was thrown");
		}
	}

	@Test(expected = IllegalStateException.class)
	public void testFinishThrows() {
		Batcher<Integer> batcher = new Batcher<>(3);
		ArrayList<Integer> items = Lists.newArrayList(1, 2, 3);
		for (Integer item : items) {
			batcher.offer(item);
		}


		batcher.close();
		batcher.close();
	}
}
