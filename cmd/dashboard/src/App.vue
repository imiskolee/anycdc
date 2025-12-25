<template>
  <a-layout>
    <a-layout-header style="height: 40px">
      <Header />
    </a-layout-header>
    <a-layout>
      <a-layout-content><Content /></a-layout-content>
    </a-layout>
    <a-layout-footer><Footer /></a-layout-footer>
  </a-layout>
</template>

<script setup>

import Header from "./layouts/Header.vue";
import Content from "./layouts/Content.vue";
import Footer from "./layouts/Footer.vue";
</script>

<style>
.a-layout-header {
  height:40px !important;
}
</style>