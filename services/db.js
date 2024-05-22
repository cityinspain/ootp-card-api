const sqlite = require('better-sqlite3')
const path = require('path')
const db = new sqlite(path.resolve("players.db"), { fileMustExist: true})

function query(sql, params) {

    if (params) {
        return db.prepare(sql).all(params)
    }

    return db.prepare(sql).all()
        
}

module.exports = {
    query
}