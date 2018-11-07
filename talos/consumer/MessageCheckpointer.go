/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package consumer

type MessageCheckpointer interface {

	/**
	 * When user call checkpoint, TalosConsumer will checkpoint the latest
	 * messageOffset(We just call it commitOffset) that had been delivered to user
	 * by MessageProcessor. Once failover happens, the new MessageProcessor will
	 * call it's interface init() with (commitOffset + 1) as the startMessageOffset,
	 * and the new MessageProcessor will deliver message with offset equals
	 * (commitOffset + 1) as the first message.
	 * User should call this method periodically (e.g. once every 1 minutes),
	 * Calling this API too frequently can slow down the application.
	 */
	CheckpointByFinishedOffset() bool

	/**
	 * Same as checkpoint, But use commotOffset as the checkpoint value.
	 * There are some restrict for commitOffset:
	 * 1. commitOffset must always greater or equal than startMessageOffset;
	 * 2. commitOffset must always greater than last commitOffset;
	 * 3. commitOffset must always littler or equal than latest deliver messageOffset.
	 */
	Checkpoint(commitOffset int64) bool
}
