import { createApp } from 'vue'
import App from './App.vue'
import Antd from 'ant-design-vue';
import AntIcon from '@ant-design/icons-vue'
import 'ant-design-vue/dist/reset.css';
import "./style.css"
import router from "./routers"
const app = createApp(App)
app.use(router)
app.use(Antd)
app.mount('#app')

