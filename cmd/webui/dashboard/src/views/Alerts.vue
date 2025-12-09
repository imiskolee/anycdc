<template>
  <table-page
      title="Alerts"
      description="Manage your alerts information(s)."
      :data="data"
      :custom-renders="customRenders"
      detail-prefix="/common/alerts"
  ></table-page>
</template>

<script setup lang="jsx">
import TablePage from "../components/TablePage.vue";

const data = [
  {
    id: "0000-00000000-0000-000000000001",
    name: "alert_1",
    task : "test_pg_task_1",
    webhookURL : "https://my.webhook.url/my/path"
  },
  {
    id: "0000-00000000-0000-000000000001",
    name: "alert_1",
    task : "test_pg_task_1",
    webhookURL : "https://my.webhook.url/my/path"
  },
  {
    id: "0000-00000000-0000-000000000001",
    name: "alert_1",
    task : "test_pg_task_1",
    webhookURL : "https://my.webhook.url/my/path"
  },
]
const customRenders = {
  "__action__": (record) => {
    return (
        <a href="#">Delete</a>
    )
  }
}




</script>