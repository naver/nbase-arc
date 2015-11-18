/*
 * Copyright 2015 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.navercorp.redis.cluster.gateway;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * RLE(Run Length Encode)
 * <p>
 * e.g) A1R2W3N4 = ARRWWWNNNN
 *
 * @author jaehong.kim
 */
public class RunLengthHelper {
    public static String decode(final String source) {
        if (source == null || source.length() < 2) {
            throw new IllegalArgumentException("source must not be empty. source=" + source);
        }

        final StringBuilder sb = new StringBuilder();
        final Pattern pattern = Pattern.compile("[A|R|W|N]|[0-9]+");
        final Matcher matcher = pattern.matcher(source);

        while (matcher.find()) {
            final String mark = matcher.group();
            if (!matcher.find()) {
                throw new IllegalArgumentException("invalid source format. source=" + source);
            }
            int number = Integer.parseInt(matcher.group());
            for (int i = 0; i < number; i++) {
                sb.append(mark);
            }
        }

        return sb.toString();
    }
}
