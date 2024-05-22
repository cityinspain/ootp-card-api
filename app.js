const express = require('express'); 
const db = require('./services/db')

const cors = require('cors')
  
const app = express(); 
app.use(cors())
const PORT = 3000;

app.get("/cards", async (req, res) => {
    const data = db.query("SELECT card_id, card_title, card_type, position, pitcher_role, first_name, last_name, year, card_value FROM cards")

    console.log(data)

    res.send(data)

    return data

})

app.get("/cards/:id", (req, res) => {

    console.log(req.params.id)

    const data = db.query("SELECT * FROM cards WHERE card_id = ?", req.params.id)

    if (data.length === 1) {
        return res.send(data[0])
    }

    return res.send(404)

})
  
app.listen(PORT, (error) =>{ 
    if(!error) 
        console.log(`Server is Successfully Running,  and App is listening on port ${PORT}`) 
    else 
        console.log("Error occurred, server can't start", error); 
    } 
); 
