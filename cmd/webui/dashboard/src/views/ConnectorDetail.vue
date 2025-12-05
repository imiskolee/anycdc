<template>
  <a-flex style="justify-content: space-between;">
    <h1>Add Connector</h1>
  </a-flex>
  <div class="form">
  <a-form layout="horizontal"
          :label-col="labelCol"
          :wrapper-col="wrapperCol"
  >
    <a-form-item label="Name" name="Name">
      <a-input />
    </a-form-item>
    <a-form-item label="Host" name="Host">
      <a-input />
    </a-form-item>
    <a-form-item label="Port" name="Port">
      <a-input />
    </a-form-item>
    <a-form-item label="Username" name="Username">
      <a-input />
    </a-form-item>
    <a-form-item label="Password" name="Password">
      <a-input />
    </a-form-item>
    <a-form-item label="Database" name="Database">
      <a-input />
    </a-form-item>
    <a-form-item label="Extras" name="Extras">
      <a-textarea style="min-height: 8em"/>
    </a-form-item>
    <a-form-item>
    </a-form-item>
    <div style="align-content: center;text-align: center">
      <a-button>提交</a-button>
    </div>
  </a-form>
  </div>
</template>
<script setup lang="ts">
const labelCol = {span:8}
const wrapperCol = { span: 16}
</script>

<style scoped>

</style>