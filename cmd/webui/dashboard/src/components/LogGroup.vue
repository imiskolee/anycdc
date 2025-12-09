<template>
  <a-tabs>
    <a-tab-pane tab="std.err.log" key="std.err.log">
      <LogPanel />
    </a-tab-pane>
    <a-tab-pane tab="std.out.log" key="std.out.log">
      <LogPanel />
    </a-tab-pane>
  </a-tabs>
</template>
<script setup>
import LogPanel from "./LogPanel.vue";
</script>
