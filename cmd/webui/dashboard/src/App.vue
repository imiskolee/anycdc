<template>
  <a-layout class="admin-layout">
    <a-layout-header class="admin-header" theme="dark">
      <div class="flex" style="display: flex">
        <div>
          <img src="/icon.svg" width="60px;">
        </div>
        <div><h2 class="header-title">AnyCDC Dashboard</h2><</div>
      </div>
    </a-layout-header>
    <a-layout style="min-height: 100vh">
      <a-layout-sider width="200">
        <Menu />
      </a-layout-sider>
      <a-layout-content class="admin-content">
        <div class="page" style="padding:20px">
          <router-view :key="$route.fullPath" />
        </div>

      </a-layout-content>
    </a-layout>
    <a-layout-footer class="admin-footer">
      <div>©{{ new Date().getFullYear() }} 后台管理系统 - 版权所有</div>
    </a-layout-footer>
  </a-layout>
  <Loading />
</template>

<script setup>
import Menu from "./Menu.vue"
import { ref } from 'vue';
import Loading from "./components/Loading.vue";

// 侧边栏折叠状态
const collapsed = ref(true);
// 当前选中菜单
const selectedKey = ref('1');
</script>
<style scoped>
.admin-header {
  color: #fff;
}
</style>
