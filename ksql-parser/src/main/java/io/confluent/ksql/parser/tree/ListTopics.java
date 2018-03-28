/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ListTopics extends Statement {

  private final Optional<QualifiedName> stream;

  public ListTopics(Optional<NodeLocation> location, Optional<QualifiedName> stream) {
    super(location);
    this.stream = stream;
  }

  public Optional<QualifiedName> getStream() {
    return stream;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ListTopics that = (ListTopics) o;
    return Objects.equals(stream, that.stream);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stream);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .toString();
  }
}
