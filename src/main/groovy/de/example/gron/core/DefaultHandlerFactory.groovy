package de.example.gron.core

import de.example.gron.api.GronException
import de.example.gron.api.TaskHandler
import de.example.gron.spi.HandlerFactory
import groovy.transform.CompileStatic

/**
 * Default {@link HandlerFactory}: instantiates handlers via their public no-arg
 * constructor.
 */
@CompileStatic
class DefaultHandlerFactory implements HandlerFactory {

    @Override
    TaskHandler create(Class<? extends TaskHandler> type) {
        try {
            return type.getDeclaredConstructor().newInstance()
        } catch (Exception e) {
            throw new GronException(
                    "Cannot instantiate handler ${type?.name} via no-arg constructor", e)
        }
    }
}
