import { createVNode, render } from 'vue';
import GlobalLoading from '../components/Loading.vue';

// 创建容器
const container = document.createElement('div');
document.body.appendChild(container);

// 创建组件实例
const vnode = createVNode(GlobalLoading, { isShow: false });
render(vnode, container);

// 定义全局方法
const loadingApi = {
    show: () => {
        vnode.props.isShow = true;
    },
    hide: () => {
        vnode.props.isShow = false;
    }
};

// 导出供全局使用
export default loadingApi;