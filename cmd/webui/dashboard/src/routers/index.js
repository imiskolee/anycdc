import { createRouter, createWebHistory } from 'vue-router';
import Home from '../views/Home.vue';
import Settings from '../views/Settings.vue';
import Connectors from "../views/Connectors.vue"
import Tasks from "../views/Tasks.vue";
import Alerts from "../views/Alerts.vue";
import Setup from "../views/Setup.vue";
import DetailPage from "../views/DetailPage.vue";
const routes = [
    { path: '/', component: Home },
    { path: '/settings', component: Settings },
    { path: '/connectors', component: Connectors },
    { path: '/tasks', component: Tasks },
    {
        path:'/alerts',component: Alerts
    },
    {
        path:'/common/:object/:id',component: DetailPage
    },
    {
        path:'/setup',component: Setup
    }
];

console.log(import.meta.env.VITE_ROOT_PATH)
const router = createRouter({
    history: createWebHistory(import.meta.env.VITE_ROOT_PATH),
    routes,
});

export default router;