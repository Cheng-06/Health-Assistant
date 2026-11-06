<script setup>
import { onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { verifyLoginAPI } from '@/api/auth'
import { getToken } from '@/utils/auth'

const router = useRouter()

// 首屏加载即校验一次登录态：token 过期时由响应拦截器统一清理并跳登录页，
// 不必等用户点进需要鉴权的页面才发现。
onMounted(async () => {
  await router.isReady()
  if (!getToken()) return
  verifyLoginAPI().catch(() => {})
})
</script>
<template>
  <div>
    <router-view> </router-view>
  </div>
</template>
