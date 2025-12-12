<template>
  <div class="dropdown-wrap">
    <a-dropdown-button @click="handleButtonClick">
      Actions
      <template #overlay>
        <a-menu @click="handleMenuClick">
          <a-menu-item v-for="item in items" :key="item.name">
            {{item.name}}
          </a-menu-item>
        </a-menu>
      </template>
    </a-dropdown-button>
  </div>
</template>


<script setup>

props = defineProps({
  items : Array
})

function handleButtonClick() {}


function handleMenuClick(e,d) {
  console.log(e,d)
}



</script>