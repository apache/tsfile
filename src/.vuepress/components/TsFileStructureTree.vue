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
import { computed, ref } from 'vue';
import TsFileStructureNodeView from './TsFileStructureNode.vue';
import {
  tsFileStructure,
  type TsFileStructureLocale,
  type TsFileStructureNode,
} from './tsFileStructure.js';

const props = withDefaults(
  defineProps<{
    locale?: TsFileStructureLocale;
  }>(),
  {
    locale: 'en',
  },
);

const messages = {
  en: {
    ariaLabel: 'Interactive TsFile v4 file structure',
    expandAll: 'Expand all',
    collapseAll: 'Collapse all',
    hint: 'Select a structure to inspect its offset and physical layout.',
    toggle: 'Toggle',
    selectedNode: 'Selected structure',
    offset: 'Offset',
    size: 'Size',
    layout: 'Physical layout',
    details: 'Open format details',
  },
  zh: {
    ariaLabel: 'TsFile v4 交互式文件结构',
    expandAll: '全部展开',
    collapseAll: '全部折叠',
    hint: '选择任一结构，查看它的偏移与物理布局。',
    toggle: '展开或折叠',
    selectedNode: '当前结构',
    offset: '偏移',
    size: '长度',
    layout: '物理布局',
    details: '查看格式详情',
  },
} as const;

const ui = computed(() => messages[props.locale]);

const initiallyExpanded = new Set([
  'tsfile',
  'file-header',
  'data-section',
  'metadata-section',
  'file-metadata',
]);
const expanded = ref(new Set(initiallyExpanded));
const selectedId = ref('tsfile');

function walk(
  node: TsFileStructureNode,
  visitor: (current: TsFileStructureNode) => void,
): void {
  visitor(node);
  node.children?.forEach((child) => walk(child, visitor));
}

const allExpandableIds = computed(() => {
  const ids: string[] = [];
  walk(tsFileStructure, (node) => {
    if (node.children?.length) ids.push(node.id);
  });
  return ids;
});

const selectedNode = computed(() => {
  let match = tsFileStructure;
  walk(tsFileStructure, (node) => {
    if (node.id === selectedId.value) match = node;
  });
  return match;
});

const selectedText = computed(() => selectedNode.value.text[props.locale]);

const detailsLink = computed(() => {
  if (!selectedNode.value.link) return undefined;
  const prefix = props.locale === 'zh' ? '/zh/UserGuide/latest/FileFormat/' : '/UserGuide/latest/FileFormat/';
  return `${prefix}${selectedNode.value.link}`;
});

function toggle(id: string): void {
  const next = new Set(expanded.value);
  if (next.has(id)) next.delete(id);
  else next.add(id);
  expanded.value = next;
}

function expandAll(): void {
  expanded.value = new Set(allExpandableIds.value);
}

function collapseAll(): void {
  expanded.value = new Set();
}
</script>

<template>
  <section class="tsfile-structure" :aria-label="ui.ariaLabel">
    <div class="tsfile-structure-toolbar">
      <p>{{ ui.hint }}</p>
      <div class="tsfile-structure-actions">
        <button type="button" @click="expandAll">
          <svg viewBox="0 0 16 16" aria-hidden="true">
            <path d="M3 5h10M3 11h10M8 2v6M8 8v6" />
          </svg>
          {{ ui.expandAll }}
        </button>
        <button type="button" @click="collapseAll">
          <svg viewBox="0 0 16 16" aria-hidden="true">
            <path d="M3 5h10M3 11h10" />
          </svg>
          {{ ui.collapseAll }}
        </button>
      </div>
    </div>

    <div class="tsfile-structure-body">
      <div class="tsfile-tree-panel">
        <ul class="tsfile-tree" role="tree" :aria-label="ui.ariaLabel">
          <TsFileStructureNodeView
            :node="tsFileStructure"
            :locale="locale"
            :expanded="expanded"
            :selected-id="selectedId"
            :level="0"
            :toggle-label="ui.toggle"
            @select="selectedId = $event"
            @toggle="toggle"
          />
        </ul>
      </div>

      <aside class="tsfile-detail-panel" aria-live="polite">
        <div class="tsfile-detail-copy">
          <p class="tsfile-detail-eyebrow">{{ ui.selectedNode }}</p>
          <h3>{{ selectedText.title }}</h3>
          <p>{{ selectedText.description }}</p>

          <p v-if="selectedText.note" class="tsfile-detail-note">
            {{ selectedText.note }}
          </p>

          <a v-if="detailsLink" class="tsfile-detail-link" :href="detailsLink">
            {{ ui.details }}
            <span aria-hidden="true">→</span>
          </a>
        </div>

        <div class="tsfile-detail-spec">
          <dl v-if="selectedText.offset || selectedText.size">
            <div v-if="selectedText.offset">
              <dt>{{ ui.offset }}</dt>
              <dd><code>{{ selectedText.offset }}</code></dd>
            </div>
            <div v-if="selectedText.size">
              <dt>{{ ui.size }}</dt>
              <dd><code>{{ selectedText.size }}</code></dd>
            </div>
          </dl>

          <div v-if="selectedText.layout" class="tsfile-detail-layout">
            <span>{{ ui.layout }}</span>
            <code>{{ selectedText.layout }}</code>
          </div>
        </div>
      </aside>
    </div>
  </section>
</template>

<style scoped>
.tsfile-structure {
  --tree-accent: var(--vp-c-accent);
  --tree-accent-bg: color-mix(in srgb, var(--tree-accent) 8%, transparent);
  --tree-line: color-mix(in srgb, var(--vp-c-border) 68%, transparent);

  container-type: inline-size;
  margin: 1.5rem 0 2rem;
  border: 1px solid var(--vp-c-border);
  border-radius: 10px;
  background: var(--vp-c-bg-elv);
}

.tsfile-structure-toolbar {
  display: flex;
  flex-wrap: wrap;
  gap: 0.75rem;
  align-items: center;
  justify-content: space-between;
  padding: 0.65rem 0.75rem;
  border-bottom: 1px solid var(--vp-c-border);
  background: var(--vp-c-bg-soft);
}

.tsfile-structure-toolbar > p {
  flex: 1 1 18rem;
  margin: 0;
  color: var(--vp-c-text-mute);
  font-size: 0.76rem;
  line-height: 1.45;
}

.tsfile-structure-actions {
  display: flex;
  gap: 0.4rem;
  align-items: center;
  flex: 0 0 auto;
}

.tsfile-structure-actions button {
  display: inline-flex;
  gap: 0.35rem;
  align-items: center;
  padding: 0.34rem 0.58rem;
  border: 1px solid var(--vp-c-border);
  border-radius: 6px;
  color: var(--vp-c-text);
  font: inherit;
  font-size: 0.78rem;
  font-weight: 600;
  background: transparent;
  cursor: pointer;
}

.tsfile-structure-actions button:hover,
.tsfile-structure-actions button:focus-visible {
  border-color: var(--tree-accent);
  color: var(--tree-accent);
  background: var(--tree-accent-bg);
}

.tsfile-structure-actions svg {
  width: 0.9rem;
  fill: none;
  stroke: currentcolor;
  stroke-linecap: round;
  stroke-width: 1.5;
}

.tsfile-tree-panel {
  min-width: 0;
  padding: 0.55rem 0;
}

.tsfile-tree,
:deep(.tsfile-tree-children) {
  padding: 0;
  margin: 0;
  list-style: none;
}

:deep(.tsfile-tree-row) {
  position: relative;
  display: flex;
  box-sizing: border-box;
  width: 100%;
  min-width: 0;
  padding-left: calc(0.52rem + var(--tree-level) * 1.05rem);
}

:deep(.tsfile-tree-row)::before {
  position: absolute;
  top: 50%;
  left: calc(0.34rem + var(--tree-level) * 1.05rem);
  width: 0.62rem;
  border-top: 1px solid var(--tree-line);
  content: '';
}

:deep(.tsfile-tree-row.is-selected) {
  background: var(--tree-accent-bg);
  box-shadow: inset 2px 0 var(--tree-accent);
}

:deep(.tsfile-tree-toggle),
:deep(.tsfile-tree-toggle-spacer) {
  z-index: 1;
  display: inline-grid;
  flex: 0 0 1.3rem;
  width: 1.3rem;
  min-height: 2.15rem;
  padding: 0;
  border: 0;
  place-items: center;
  color: var(--vp-c-text-mute);
  background: transparent;
}

:deep(.tsfile-tree-toggle) {
  cursor: pointer;
}

:deep(.tsfile-tree-toggle:hover),
:deep(.tsfile-tree-toggle:focus-visible) {
  color: var(--tree-accent);
}

:deep(.tsfile-tree-chevron) {
  width: 0.9rem;
  fill: none;
  stroke: currentcolor;
  stroke-linecap: round;
  stroke-linejoin: round;
  stroke-width: 1.6;
  transition: transform 0.16s ease;
}

:deep(.tsfile-tree-chevron.is-open) {
  transform: rotate(90deg);
}

:deep(.tsfile-tree-label) {
  display: block;
  flex: 1 1 auto;
  width: auto;
  min-width: 0;
  padding: 0.42rem 0.7rem 0.42rem 0.12rem;
  border: 0;
  color: var(--vp-c-text);
  font: inherit;
  text-align: left;
  background: transparent;
  cursor: pointer;
}

:deep(.tsfile-tree-label:hover .tsfile-tree-title),
:deep(.tsfile-tree-label:focus-visible .tsfile-tree-title) {
  color: var(--tree-accent);
}

:deep(.tsfile-tree-title) {
  min-width: 0;
  font-size: 0.88rem;
  font-weight: 650;
  line-height: 1.35;
  overflow-wrap: anywhere;
}

.tsfile-detail-panel {
  display: grid;
  grid-template-columns: repeat(
    auto-fit,
    minmax(min(100%, 16rem), 1fr)
  );
  gap: 1rem 1.5rem;
  padding: 1rem 1.1rem;
  border-top: 1px solid var(--vp-c-border);
  background: color-mix(in srgb, var(--tree-accent) 3%, var(--vp-c-bg-elv));
}

.tsfile-detail-eyebrow {
  margin: 0 0 0.35rem;
  color: var(--tree-accent);
  font-size: 0.68rem;
  font-weight: 750;
  letter-spacing: 0.09em;
  text-transform: uppercase;
}

.tsfile-detail-panel h3 {
  padding: 0;
  margin: 0;
  border: 0;
  font-size: 1.14rem;
}

.tsfile-detail-copy > p:not(.tsfile-detail-eyebrow) {
  margin: 0.7rem 0;
  color: var(--vp-c-text-mute);
  font-size: 0.9rem;
  line-height: 1.65;
}

.tsfile-detail-panel dl {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 0.55rem;
  margin: 0 0 0.7rem;
}

.tsfile-detail-panel dl > div {
  min-width: 0;
}

.tsfile-detail-panel dt {
  color: var(--vp-c-text-mute);
  font-size: 0.82rem;
  font-weight: 650;
}

.tsfile-detail-panel dd {
  min-width: 0;
  margin: 0.18rem 0 0;
  font-size: 0.9rem;
}

.tsfile-detail-panel dd code {
  overflow-wrap: anywhere;
}

.tsfile-detail-layout {
  display: grid;
  gap: 0.4rem;
  padding: 0.7rem;
  border: 1px solid var(--vp-c-border);
  border-radius: 7px;
  background: var(--vp-c-bg-soft);
}

.tsfile-detail-layout span {
  color: var(--vp-c-text-mute);
  font-size: 0.76rem;
  font-weight: 650;
}

.tsfile-detail-layout code {
  padding: 0;
  color: var(--vp-c-text);
  font-size: 0.86rem;
  line-height: 1.55;
  overflow-wrap: anywhere;
  background: transparent;
}

.tsfile-detail-note {
  padding-left: 0.65rem;
  border-left: 2px solid var(--tree-accent);
}

.tsfile-detail-link {
  display: inline-flex;
  gap: 0.35rem;
  align-items: center;
  margin-top: 0.8rem;
  font-size: 0.78rem;
  font-weight: 650;
}

@container (max-width: 38rem) {
  .tsfile-structure-toolbar {
    align-items: flex-start;
    flex-direction: column;
  }
}
</style>
