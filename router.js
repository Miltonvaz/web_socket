const express = require("express")

const router = express.Router();

router.get('/', (req,res)=>{
    res.send("web socket activo")
})

module.exports = router;