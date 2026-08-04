<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

<script setup lang="ts">
import type {
  TsFileStructureLocale,
  TsFileStructureNode,
} from './tsFileStructure.js';

defineOptions({ name: 'TsFileStructureNode' });

defineProps<{
  node: TsFileStructureNode;
  locale: TsFileStructureLocale;
  expanded: Set<string>;
  selectedId: string;
  level: number;
  toggleLabel: string;
}>();

defineEmits<{
  select: [id: string];
  toggle: [id: string];
}>();
</script>

<template>
  <li
    class="tsfile-tree-node"
    role="treeitem"
    :aria-expanded="node.children?.length ? expanded.has(node.id) : undefined"
    :aria-selected="selectedId === node.id"
  >
    <div
      class="tsfile-tree-row"
      :class="[
        `is-${node.kind}`,
        { 'is-selected': selectedId === node.id },
      ]"
      :style="{ '--tree-level': level }"
    >
      <button
        v-if="node.children?.length"
        class="tsfile-tree-toggle"
        type="button"
        :aria-label="`${toggleLabel}: ${node.text[locale].title}`"
        :aria-expanded="expanded.has(node.id)"
        @click="$emit('toggle', node.id)"
      >
        <svg
          class="tsfile-tree-chevron"
          :class="{ 'is-open': expanded.has(node.id) }"
          viewBox="0 0 16 16"
          aria-hidden="true"
        >
          <path d="M5.5 3.5 10 8l-4.5 4.5" />
        </svg>
      </button>
      <span v-else class="tsfile-tree-toggle-spacer" aria-hidden="true"></span>

      <button
        class="tsfile-tree-label"
        type="button"
        @click="$emit('select', node.id)"
      >
        <span class="tsfile-tree-title">{{ node.text[locale].title }}</span>
      </button>
    </div>

    <ul
      v-if="node.children?.length && expanded.has(node.id)"
      class="tsfile-tree-children"
      role="group"
    >
      <TsFileStructureNode
        v-for="child in node.children"
        :key="child.id"
        :node="child"
        :locale="locale"
        :expanded="expanded"
        :selected-id="selectedId"
        :level="level + 1"
        :toggle-label="toggleLabel"
        @select="$emit('select', $event)"
        @toggle="$emit('toggle', $event)"
      />
    </ul>
  </li>
</template>
