/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package producer

/**
 * Note: this interface is not thread-safe
 */

type UserMessageCallback interface {

	/**
	 * Implement this method to process messages that successfully put to server
	 * user can get 'messageList', 'partitionId' and 'isSuccessful' by userMessageResult
	 */
	OnSuccess(userMessageResult UserMessageResult)

	/**
	 * Implement this method to process messages failed to put to server
	 * user can get 'messageList', 'partitionId' and 'cause' by userMessageResult
	 */
	OnError(userMessageResult UserMessageResult)
}
