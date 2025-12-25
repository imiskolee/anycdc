<template>
<div class="content-view">
  <router-view />
</div>
</template>
<script setup>
import {RouterView} from "vue-router";
</script>

<style scoped>
.content-view {
  padding:10px;
}
</style>