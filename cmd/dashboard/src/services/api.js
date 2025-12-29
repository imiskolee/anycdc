
import {notification } from 'ant-design-vue';
class APISDK {
    /**
     * 初始化 SDK
     * @param {Object} config 配置项
     * @param {string} config.domain API 域名（必填，如 https://api.example.com）
     * @param {number} [config.timeout=10000] 请求超时时间（毫秒）
     * @param {Object} [config.headers={}] 全局请求头
     */
    constructor(config) {
        // 校验必填配置
        if (!config?.domain) {
            config.domain = import.meta.env.VITE_API_URL
        }
        // 初始化基础配置
        this.domain = config.domain.replace(/\/$/, ''); // 移除域名末尾的斜杠
        this.timeout = config.timeout || 10000;
        this.headers = {
            'Content-Type': 'application/json',
            ...config.headers
        };
    }

    /**
     * 通用请求方法
     * @param {string} path 请求路径
     * @param {string} method HTTP 方法
     * @param {Object} [data=null] 请求体数据
     * @returns {Promise<Object>} 响应数据
     */
    async _request(path, method, data = null) {
        const url = `${this.domain}${path}`;
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), this.timeout);

        try {
            // 构建请求配置
            const requestConfig = {
                method,
                headers: this.headers,
                signal: controller.signal
            };

            // POST/PUT 请求添加请求体
            if (data && ['POST', 'PUT', 'PATCH'].includes(method)) {
                requestConfig.body = JSON.stringify(data);
            }

            // 发送请求
            const response = await fetch(url, requestConfig);
            clearTimeout(timeoutId);

            // 解析响应数据
            const responseData = await response.json();

            // 处理后台错误格式
            if (responseData.error) {
                throw new Error(`API 错误 [${responseData.error}]: ${responseData.msg}`);
            }
            if(method !== 'GET') {
                notification.info(
                    {
                        message: method + " " + path,
                        description: "Operation successful."
                    }
                )
            }
            // 返回成功数据
            return responseData.data || {};
        } catch (error) {
            clearTimeout(timeoutId);
            notification.error(
                {
                    message : method + " " + path + " failed",
                    description : error.message
                }
            )
            if (error.name === 'AbortError') {
                throw new Error(`请求超时（${this.timeout}ms）：${url}`);
            }
            throw new Error(`请求失败：${error.message}`);
        }
    }
    /**
     * 创建数据
     * @param {string} tableName 表名
     * @param {Object} obj 要创建的数据对象
     * @returns {Promise<Object>} 创建的详情数据
     */
    async Create(tableName, obj) {
        if (!tableName || typeof tableName !== 'string') {
            throw new Error('参数错误：tableName 必须是有效字符串');
        }
        if (!obj || typeof obj !== 'object' || Array.isArray(obj)) {
            throw new Error('参数错误：obj 必须是非数组的对象');
        }
        return this._request(`/${tableName}`, 'POST', obj);
    }

    /**
     * 更新数据
     * @param {string} tableName 表名
     * @param {string|number} id 数据ID
     * @param {Object} obj 要更新的数据对象
     * @returns {Promise<Object>} 更新后的详情数据
     */
    async Update(tableName, id, obj) {
        if (!tableName || typeof tableName !== 'string') {
            throw new Error('参数错误：tableName 必须是有效字符串');
        }
        if (!id && id !== 0) {
            throw new Error('参数错误：id 不能为空');
        }
        if (!obj || typeof obj !== 'object' || Array.isArray(obj)) {
            throw new Error('参数错误：obj 必须是非数组的对象');
        }
        return this._request(`/${tableName}/${id}`, 'PUT', obj);
    }

    /**
     * 删除数据
     * @param {string} tableName 表名
     * @param {string|number} id 数据ID
     * @returns {Promise<Object>} 删除结果
     */
    async Delete(tableName, id) {
        if (!tableName || typeof tableName !== 'string') {
            throw new Error('参数错误：tableName 必须是有效字符串');
        }
        if (!id && id !== 0) {
            throw new Error('参数错误：id 不能为空');
        }
        return this._request(`/${tableName}/${id}`, 'DELETE');
    }

    /**
     * 获取单条数据
     * @param {string} tableName 表名
     * @param {string|number} id 数据ID
     * @returns {Promise<Object>} 数据详情
     */
    async Get(tableName, id) {
        if (!tableName || typeof tableName !== 'string') {
            throw new Error('参数错误：tableName 必须是有效字符串');
        }
        if (!id && id !== 0) {
            throw new Error('参数错误：id 不能为空');
        }
        return this._request(`/${tableName}/${id}`, 'GET');
    }

    /**
     * 获取列表数据
     * @param {string} tableName 表名
     * @returns {Promise<Object>} 列表数据
     */
    async List(tableName) {
        if (!tableName || typeof tableName !== 'string') {
            throw new Error('参数错误：tableName 必须是有效字符串');
        }
        return this._request(`/${tableName}`, 'GET');
    }

    async StartTask(id )   {
        return this._request(`/tasks/${id}/start`, 'PUT');
    }
    async StopTask(id )   {
        return this._request(`/tasks/${id}/stop`, 'PUT');
    }
    async GetTaskLog(id) {
        return this._request(`/tasks/${id}/logs`, 'GET');
    }
    async ActiveTask(id )   {
        return this._request(`/tasks/${id}/active`, 'PUT');
    }
    async InactiveTask(id) {
        return this._request(`/tasks/${id}/inactive`, 'PUT');
    }
    async TestConnector(data )   {
        return this._request(`/utils/test_connector`, 'POST',data);
    }
    async LogTail(data)   {
        return this._request(`/utils/log_tail`, 'POST',data);
    }
    async GetTaskTableLogs(id)   {
        return this._request(`/tasks/${id}/table_logs`, 'GET');
    }
    async TaskRotateTo(id,data) {
        return this._request(`/tasks/${id}/rotate`, 'PUT',data);
    }
    async TaskTableResync(id,lastDumperKey) {
        return this._request(`/task_tables/${id}/resync`, 'PUT',{
            last_dumper_key : lastDumperKey
        });
    }
}

export default new APISDK({})