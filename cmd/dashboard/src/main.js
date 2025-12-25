import { createApp } from 'vue'
import App from './App.vue'
import Antd from 'ant-design-vue';
import 'ant-design-vue/dist/reset.css';
import "./style.css"
import routers from "./routers.js";
const app = createApp(App)
app.use(Antd)
app.use(routers)
app.mount('#app')

