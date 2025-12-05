import { createRouter, createWebHistory } from 'vue-router';
import Home from '../views/Home.vue';
import Settings from '../views/Settings.vue';
import Connectors from "../views/Connectors.vue"
import Tasks from "../views/Tasks.vue"
import ConnectorDetail  from "../views/ConnectorDetail.vue";
import TaskDetail from "../views/TaskDetail.vue";
const routes = [
    { path: '/', component: Home },
    { path: '/settings', component: Settings },
    { path: '/connectors', component: Connectors },
    { path: '/tasks', component: Tasks },
    {
        path:'/connectors/:id',component: ConnectorDetail
    },
    {
        path:'/tasks/:id',component: TaskDetail
    }
];

const router = createRouter({
    history: createWebHistory(),
    routes,
});

export default router;