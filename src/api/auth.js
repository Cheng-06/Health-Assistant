import service from '@/utils/request'

export function loginAPI(data) {
  return service.post('/user/login', data)
}

export function registerAPI(data) {
  return service.post('/user/add', data)
}

export function logoutAPI() {
  return service.post('/user/logout')
}

// 登录态探针：后端没有独立的校验接口，
// 这里用一个必须校验 token 的只读接口做轻量探测。
// token 失效时后端统一返回 403，由响应拦截器清理登录态并跳登录页；
// silent 用于避免启动时网络抖动弹出无关提示。
export function verifyLoginAPI() {
  return service.get('/psychological-chat/sessions', {
    params: { currentPage: 1, page: 1, pageSize: 1 },
    silent: true
  })
}
