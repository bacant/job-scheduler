package de.example.gron.spi

import de.example.gron.api.TaskHandler

/**
 * Instantiates {@link TaskHandler} objects. This is the dependency-injection
 * attachment point; the default implementation uses the no-arg constructor.
 */
interface HandlerFactory {

    /**
     * Creates a handler instance for the given type.
     *
     * @param type the handler class to instantiate
     * @return a ready-to-use handler
     */
    TaskHandler create(Class<? extends TaskHandler> type)
}
