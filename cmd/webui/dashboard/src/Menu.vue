<template>
  <div>
    <a-menu
        v-model:openKeys="openKeys"
        v-model:selectedKeys="selectedKeys"
        @select="handleMenuSelect"
        style="width: 200px"
        mode="inline"
        theme="dark"
        :items="items"
    />
  </div>
</template>
<script lang="ts" setup>
import { h, ref } from 'vue';
import {
  MailOutlined,
  CalendarOutlined,
  AppstoreOutlined,
  SettingOutlined,
} from '@ant-design/icons-vue';
import type { MenuTheme } from 'ant-design-vue';
import { useRouter, useRoute } from 'vue-router';
const router = useRouter(); // 路由实例（用于跳转）
const theme = ref<MenuTheme>('dark');
const selectedKeys = ref(['1']);
const openKeys = ref(['sub1']);
const items = ref([
  {
    key: '1',
    icon: () => h(MailOutlined),
    label: 'Dashboard',
    title: 'Dashboard',
    link : "/"
  },
  {
    key: '2',
    icon: () => h(CalendarOutlined),
    label: 'Connectors',
    link: "/connectors"
  },
  {
    key: '3',
    icon: () => h(CalendarOutlined),
    label: 'Tasks',
    link : "/tasks"

  },
  {
    key: '4',
    icon: () => h(SettingOutlined),
    label: 'Settings',
    link:"/settings"
  },
]);
function handleMenuSelect(v) {
  const navTo = v.item.originItemValue
  router.push(navTo.link)
}
</script>

