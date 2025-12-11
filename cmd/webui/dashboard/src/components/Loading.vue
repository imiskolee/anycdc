<template>
  <teleport to="body"> <!-- 挂载到 body 下，避免被父组件样式影响 -->
    <a-spin
        :spinning="isShow"
        fullscreen
        tip="正在加载..."
        size="large"
    />
  </teleport>
</template>

<script setup lang="ts">
import { defineProps, defineEmits } from 'vue';
import { Spin } from 'ant-design-vue'
// 接收外部传值控制显示
const props = defineProps({
  isShow: {
    type: Boolean,
    default: false
  }
});
</script>