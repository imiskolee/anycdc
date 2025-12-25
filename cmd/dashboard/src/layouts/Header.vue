<template>
  <div style="display: flex;justify-content: start">
    <h3 class="header-title" style="line-height: 40px">AnyCDC Dashboard</h3>
    <a-menu mode="horizontal" v-model:selectedKeys="selectedKeys" :items="items"  :style="{ lineHeight: '40px', width:'80%',height:'40px'}" theme="dark" >
    </a-menu>
  </div>
</template>
<script setup>
import {h, onMounted, ref} from "vue";
import {HomeOutlined, InsertRowAboveOutlined, ScheduleOutlined, SettingOutlined} from "@ant-design/icons-vue";
import { useRoute } from 'vue-router';

const selectedKeys = ref([])
const items = [
  {
    label: h('a',{href:'/ui'},'Home'),
    title : "Home",
    key : "/ui",
    icon:()=>h(HomeOutlined),
  },
  {
    label : h('a',{href:'/ui/connectors'},'Connectors'),
    title: "Connectors",
    key : "/ui/connectors",
    icon:()=>h(InsertRowAboveOutlined),
  },
  {
    label : h('a',{href:'/ui/tasks'},'Tasks'),
    title: "Tasks",
    icon:()=>h(ScheduleOutlined),
    key : "/ui/tasks",
  },
  {
    label : h('a',{href:'/ui/settings'},'Settings'),
    title: "Settings",
    icon: ()=>h(SettingOutlined),
    key : "/ui/settings",
  }
]

onMounted(()=>{
  const route = useRoute();
  for(let k in items) {
    console.log(window.location.pathname)
    if(window.location.pathname === items[k].key) {
      selectedKeys.value = [items[k].key]
    }
  }
})





</script>

<style >
.header-title {
  color: white;
}
.a-layout-header {
  height:40px !important;
}
</style>