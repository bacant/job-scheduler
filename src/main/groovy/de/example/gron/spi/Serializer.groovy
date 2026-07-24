package de.example.gron.spi

import de.example.gron.api.Task

/**
 * JSON (de)serialization SPI for tasks and generic documents. The default
 * implementation uses {@code groovy.json}. There is deliberately no separate
 * schedule serializer — schedules are encoded inline within the task document
 * via a type tag.
 */
interface Serializer {

    /** Serializes a task (including its schedule and options) to a JSON string. */
    String taskToJson(Task task)

    /**
     * Restores a task from JSON, resolving the handler class via the given
     * class loader.
     */
    Task taskFromJson(String json, ClassLoader classLoader)

    /** Encodes a generic document (used for change-event payloads). */
    String encode(Map<String, Object> document)

    /** Decodes a generic document previously produced by {@link #encode}. */
    Map<String, Object> decode(String json)
}
