// services/redisExpiryService.js

let expirySubscriber = null;

export const startRedisExpiryService = async (redisClient) => {
  try {
    if (!redisClient) {
      throw new Error("Redis client is required");
    }

    console.log(
      "[Redis Expiry] Starting expiry service..."
    );

    // ------------------------------------------------
    // Create separate Redis connection
    // ------------------------------------------------

    expirySubscriber = redisClient.duplicate();

    expirySubscriber.on("error", (error) => {
      console.error(
        "[Redis Expiry] Subscriber error:",
        error
      );
    });

    expirySubscriber.on("connect", () => {
      console.log(
        "[Redis Expiry] Subscriber connected"
      );
    });

    expirySubscriber.on("ready", () => {
      console.log(
        "[Redis Expiry] Subscriber ready"
      );
    });

    expirySubscriber.on("end", () => {
      console.log(
        "[Redis Expiry] Subscriber connection ended"
      );
    });

    await expirySubscriber.connect();

    console.log(
      "[Redis Expiry] Subscriber Redis connection established"
    );

    // ------------------------------------------------
    // Subscribe to expired keys
    // ------------------------------------------------
    //
    // Using * instead of 0 means:
    // __keyevent@0__:expired
    // __keyevent@1__:expired
    // etc.
    //
    // This avoids DB-number problems.
    // ------------------------------------------------

    await expirySubscriber.pSubscribe(
      "__keyevent@*__:expired",
      async (expiredKey, channel) => {
        try {
          console.log(
            "=========================================="
          );

          console.log(
            "[Redis Expiry] EXPIRED KEY:",
            expiredKey
          );

          console.log(
            "[Redis Expiry] CHANNEL:",
            channel
          );

          console.log(
            "=========================================="
          );

          // ------------------------------------------
          // Only process cleanup keys
          // ------------------------------------------

          if (!expiredKey.startsWith("cleanup_:")) {
            console.log(
              "[Redis Expiry] Ignoring key:",
              expiredKey
            );

            return;
          }

          await handleCleanupKeyExpired(
            redisClient,
            expiredKey
          );
        } catch (error) {
          console.error(
            "[Redis Expiry] Handler error:",
            error
          );
        }
      }
    );

   
  } catch (error) {
    console.error(
      "[Redis Expiry] Failed to start:",
      error
    );

    throw error;
  }
};


// ==================================================
// HANDLE EXPIRED CLEANUP KEY
// ==================================================

// ==================================================
// HANDLE EXPIRED CLEANUP KEY
// ==================================================

const handleCleanupKeyExpired = async (
  redisClient,
  expiredKey,
) => {
  try {
    console.log(
      "[Redis Cleanup] Processing:",
      expiredKey,
    );

    // =================================================
    // VALIDATE PREFIX
    // =================================================

    const prefix = "cleanup_:";

    if (!expiredKey.startsWith(prefix)) {
     

      return;
    }

    // =================================================
    // PARSE CLEANUP KEY
    // =================================================

    /**
     * Expected:
     *
     * cleanup_:roomId_astrologerId_userId
     */

    const keyData = expiredKey.substring(
      prefix.length,
    );

    // UUIDs contain "-" but not "_"
    const parts = keyData.split("_");

    if (parts.length !== 3) {
      console.error(
        "[Redis Cleanup] Invalid cleanup key:",
        expiredKey,
      );

      console.error(
        "[Redis Cleanup] Parts:",
        parts,
      );

      return;
    }

    const [
      roomId,
      astrologerId,
      userId,
    ] = parts;
    

    // =================================================
    // CHECK ASTROLOGER PRESENCE
    // =================================================

    const presenceKey =
      `presence:astro:${astrologerId}`;

    const presenceData =
      await redisClient.get(presenceKey);

    // =================================================
    // PRESENCE KEY NOT FOUND
    // =================================================

    if (!presenceData) {
      return;
    }

    // =================================================
    // PARSE PRESENCE DATA
    // =================================================

    let presence;

    try {
      presence = JSON.parse(presenceData);
    } catch (error) {
      console.error(
        "[Redis Cleanup] Invalid presence JSON:",
        presenceData,
      );

      return;
    }

    console.log(
      "[Redis Cleanup] Presence data:",
      presence,
    );

    // =================================================
    // CHECK APP STATE
    // =================================================

    const appState = presence?.appState;

    console.log(
      "[Redis Cleanup] App State:",
      appState,
    );

    // =================================================
    // APP IS NOT BACKGROUND
    // =================================================

    if (appState !== "background" || "inactive") {

      return;
    }


    // =================================================
    // DELETE ROOM RELATED KEYS
    // =================================================

    const keysToDelete = [
      `request_data:${roomId}`,
      `active_chat:${roomId}`,
      `active_call:${roomId}`,
    ];

    console.log(
      "[Redis Cleanup] Deleting keys:",
      keysToDelete,
    );

    const deletedCount =
      await redisClient.del(keysToDelete);

    console.log(
      "[Redis Cleanup] Deleted count:",
      deletedCount,
    );

    // =================================================
    // REMOVE USER FROM QUEUE
    // =================================================

    const queueKey =
      `queue:${astrologerId}`;

    const queueData =
      await redisClient.lRange(
        queueKey,
        0,
        -1,
      );
   

    for (const item of queueData) {
      try {
        const parsed =
          JSON.parse(item);

        if (
          parsed.roomId === roomId ||
          parsed.room_id === roomId ||
          parsed.user_id === userId
        ) {
          const removed =
            await redisClient.lRem(
              queueKey,
              0,
              item,
            );
         
        }
      } catch (error) {
        console.error(
          "[Redis Cleanup] Invalid queue item:",
          item,
        );
      }
    }

    // =================================================
    // REMOVE USER FROM QUEUE SET
    // =================================================

    const userQueueKey =
      `user_in_queue:${astrologerId}`;

    const removedFromSet =
      await redisClient.sRem(
        userQueueKey,
        userId,
      );

    // =================================================
    // CLEAR CURRENT CHAT/CALL ONLY IF SAME ROOM
    // =================================================

    const currentChatKey =
      `current_chat:${astrologerId}`;

    const currentCallKey =
      `current_call:${astrologerId}`;

    const [
      currentChatRoom,
      currentCallRoom,
    ] = await Promise.all([
      redisClient.get(currentChatKey),
      redisClient.get(currentCallKey),
    ]);

    if (currentChatRoom === roomId) {
      await redisClient.del(currentChatKey);
      
    }

    if (currentCallRoom === roomId) {
      await redisClient.del(currentCallKey);
     
    }
    
   
  } catch (error) {
    console.error(
      "[Redis Cleanup] Error:",
      error,
    );
  }
};