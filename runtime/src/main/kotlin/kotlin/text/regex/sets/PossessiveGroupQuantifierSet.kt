/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package kotlin.text

/**
 * Possessive quantifier set over groups.
 *
 * @author Nikolay A. Kuznetsov
 */
internal class PossessiveGroupQuantifierSet(
        quantifier: Quantifier,
        innerSet: AbstractSet,
        next: AbstractSet,
        type: Int,
        setCounter: Int
): GroupQuantifierSet(quantifier, innerSet, next, type, setCounter) {

    init {
        innerSet.next = FSet.possessiveFSet // TODO: Try to use such an approach for other sets.
    }

    override fun matches(startIndex: Int, testString: CharSequence, matchResult: MatchResultImpl): Int {

        var index = startIndex
        var nextIndex: Int = innerSet.matches(index, testString, matchResult)
        var occurrences = 0
        while (nextIndex > index && (max == Quantifier.INF || occurrences < max)) {
            occurrences++
            index = nextIndex
            nextIndex = innerSet.matches(index, testString, matchResult)
        }

        // TODO: Do we need this check?
        if (/* nextIndex < 0 && */ occurrences < quantifier.min) {
            return -1
        } else {
            return next.matches(index, testString, matchResult)
        }
    }
}
