import Home from "./views/Home.vue";
import ConnectorList from "./views/ConnectorList.vue";
import ConnectorDetail from "./views/ConnectorDetail.vue";
import TaskDetail from "./views/TaskDetail.vue";
import TaskList from "./views/TaskList.vue";
import {createRouter, createWebHistory} from "vue-router";

const routes = [
    {
        path : "/",
        component : Home
    },
    {
        path : "/connectors",
        component: ConnectorList,
    },
    {
        path : "/connectors/:id",
        component: ConnectorDetail,
    },
    {
      path : "/tasks",
      component: TaskList,
    },
    {
        path : "/tasks/:id",
        component: TaskDetail,
    },
]

const router = createRouter({
    history: createWebHistory(import.meta.env.VITE_ROOT_PATH),
    routes,
});

export default router;