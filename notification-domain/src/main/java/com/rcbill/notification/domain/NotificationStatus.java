package com.rcbill.notification.domain;

/** Persistent task states; transition rules are a later TDD increment. */
public enum NotificationStatus {
  PENDING,
  IN_FLIGHT,
  RETRY_WAIT,
  SUCCEEDED,
  DEAD,
  EXPIRED
}
