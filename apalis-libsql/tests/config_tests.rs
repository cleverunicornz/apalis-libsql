//! Tests for configuration functionality

use apalis_libsql::Config;
use std::time::Duration;

#[test]
fn test_config_new() {
    let config = Config::new("test_queue");
    assert_eq!(config.queue().to_string(), "test_queue");
    assert_eq!(config.buffer_size(), 10);
    assert_eq!(config.poll_interval(), Duration::from_millis(100));
    assert_eq!(config.keep_alive(), Duration::from_secs(30));
    assert_eq!(config.reenqueue_orphaned_after(), Duration::from_secs(300));
}

#[test]
fn test_config_set_buffer_size() {
    let config = Config::new("test_queue").set_buffer_size(25);
    assert_eq!(config.buffer_size(), 25);
}

#[test]
#[should_panic(expected = "Buffer size must be greater than 0")]
fn test_config_set_buffer_size_zero_panics() {
    let _config = Config::new("test_queue").set_buffer_size(0);
}

#[test]
fn test_config_set_poll_interval() {
    let config = Config::new("test_queue").set_poll_interval(Duration::from_millis(500));
    assert_eq!(config.poll_interval(), Duration::from_millis(500));
}

#[test]
fn test_config_set_keep_alive() {
    let config = Config::new("test_queue").set_keep_alive(Duration::from_secs(60));
    assert_eq!(config.keep_alive(), Duration::from_secs(60));
}

#[test]
fn test_config_set_reenqueue_orphaned_after() {
    let config = Config::new("test_queue").set_reenqueue_orphaned_after(Duration::from_secs(600));
    assert_eq!(config.reenqueue_orphaned_after(), Duration::from_secs(600));
}

#[test]
fn test_config_getters() {
    let config = Config::new("test_queue")
        .set_buffer_size(20)
        .set_poll_interval(Duration::from_millis(200))
        .set_keep_alive(Duration::from_secs(45))
        .set_reenqueue_orphaned_after(Duration::from_secs(500));

    assert_eq!(config.queue().to_string(), "test_queue");
    assert_eq!(config.buffer_size(), 20);
    assert_eq!(config.poll_interval(), Duration::from_millis(200));
    assert_eq!(config.keep_alive(), Duration::from_secs(45));
    assert_eq!(config.reenqueue_orphaned_after(), Duration::from_secs(500));
}

#[test]
fn test_config_chaining() {
    let config = Config::new("test_queue")
        .set_buffer_size(15)
        .set_poll_interval(Duration::from_millis(150))
        .set_keep_alive(Duration::from_secs(35))
        .set_reenqueue_orphaned_after(Duration::from_secs(400));

    assert_eq!(config.buffer_size(), 15);
    assert_eq!(config.poll_interval(), Duration::from_millis(150));
    assert_eq!(config.keep_alive(), Duration::from_secs(35));
    assert_eq!(config.reenqueue_orphaned_after(), Duration::from_secs(400));
}

#[test]
fn test_config_clone() {
    let config1 = Config::new("test_queue")
        .set_buffer_size(30)
        .set_poll_interval(Duration::from_millis(250));

    let config2 = config1.clone();

    assert_eq!(config2.queue().to_string(), "test_queue");
    assert_eq!(config2.buffer_size(), 30);
    assert_eq!(config2.poll_interval(), Duration::from_millis(250));
    assert_eq!(config2.keep_alive(), Duration::from_secs(30)); // default
    assert_eq!(config2.reenqueue_orphaned_after(), Duration::from_secs(300)); // default
}

#[test]
fn test_config_debug() {
    let config = Config::new("test_queue").set_buffer_size(42);
    let debug_str = format!("{:?}", config);
    assert!(debug_str.contains("Config"));
    assert!(debug_str.contains("test_queue"));
}
