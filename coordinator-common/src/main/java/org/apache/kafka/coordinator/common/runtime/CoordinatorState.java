/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.coordinator.common.runtime;

/**
 * The various state that a coordinator for a partition can be in.
 */
// Based on the last commit ID of trunk: 5f0e368255b6c3638a7502ea787eed17ca69cf18
public enum CoordinatorState {
    /**
     * Initial state when a coordinator is created.
     */
    INITIAL {
        @Override
        boolean canTransitionFrom(CoordinatorState state) {
            return false;
        }
    },

    /**
     * The coordinator is being loaded.
     */
    LOADING {
        @Override
        boolean canTransitionFrom(CoordinatorState state) {
            return state == INITIAL || state == FAILED;
        }
    },

    /**
     * The coordinator is active and can service requests.
     */
    ACTIVE {
        @Override
        boolean canTransitionFrom(CoordinatorState state) {
            return state == ACTIVE || state == LOADING;
        }
    },

    /**
     * The coordinator is closed.
     */
    CLOSED {
        @Override
        boolean canTransitionFrom(CoordinatorState state) {
            return true;
        }
    },

    /**
     * The coordinator loading has failed.
     */
    FAILED {
        @Override
        boolean canTransitionFrom(CoordinatorState state) {
            return state == LOADING || state == ACTIVE;
        }
    };

    abstract boolean canTransitionFrom(CoordinatorState state);
}
