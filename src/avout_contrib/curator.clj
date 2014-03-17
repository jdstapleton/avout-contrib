(ns avout-contrib.curator
  (:import (org.apache.curator.retry BoundedExponentialBackoffRetry))
  (:require [avout.client-handle :as client]
            [zookeeper.internal :as zi])
  (:import [org.apache.curator CuratorZookeeperClient]
           [org.apache.curator.retry BoundedExponentialBackoffRetry]))

(defn bounded-exponetial-backoff-retry
  "Retry policy that retries a set number of times with an increasing (up to a maximum bound) sleep time between retries"
  [base-sleep-time-ms max-sleep-time-ms max-retries]
  (BoundedExponentialBackoffRetry. base-sleep-time-ms max-sleep-time-ms max-retries))

(defn make-curator-zookeeper-client-handle
  "Creates a new ZooKeeper client handle that simply wraps a single CuratorZooKeeperClient object.
  Calls to getClient will always return a Zookeeper instance that is connected and valid, unless an
  unrecoverable exception occured."
  [connection-string & {:keys [session-timeout-ms connection-timeout-ms watcher retry-policy]
                        :or   {session-timeout-ms    5000
                               connection-timeout-ms 10000
                               watcher               nil
                               retry-policy          (bounded-exponetial-backoff-retry 500 10000 20)}}]
  (let [curator (CuratorZookeeperClient. connection-string session-timeout-ms connection-timeout-ms
                                         (when watcher (zi/make-watcher watcher)) retry-policy)]
    (.start curator)
    (reify client/ClientHandle
      (getClient [this]
        (.blockUntilConnectedOrTimedOut curator)
        (.getZooKeeper curator)))))