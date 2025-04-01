let ts0 = +(new Date())
let regexPath = new RegExp(/^\/[-a-z0-9]+$/)
let regexName = new RegExp(/^[-A-Za-z0-9\s"']$/)

$(document).ready(function(){
  $('#noaava').DataTable({
    initComplete:()=>{
      let ts1 = +(new Date())
      console.log(`loaded in ${(ts1-ts0)/1000}s`)
    },
    ajax: 'data/noaa-voices.json',
    order: {
      name:'date',
      dir:'desc'
    },
    columns: [
      
      {data:'title', name:'title', title:'interviewee', render:data=>{
        return `<a href="https://web.archive.org/web/2/https://voices.nmfs.noaa.gov${data.href}" target="_blank">${data.text}</a>`
      }},
      
      {data:'interviewers', name:'interviewers', title:'interviewer', render:data=>{
        return data.map(o=>`<a href="https://web.archive.org/web/2/https://voices.nmfs.noaa.gov${o.href}" target="_blank">${o.text}</a>`
        ).join('<br/>')
      }},
      
      {data:'date', name:'date', title:'date of interview', render:data=>{
        if(data.text.trim().length) {
          let dates = data.text.trim().split(',').map(date=>date.trim())
          let datesISO = dates.map(date=>{
            let [mo,da,yr] = date.split('-')
            return [yr,mo,da].join('-')
          })
          return datesISO.join('<br/>')
        } else return ''
      }},

      {data:'affiliations', name:'affiliations', title:'contributing organisation', render:data=>{
        return data.map(o=>`<a href="https://web.archive.org/web/2/https://voices.nmfs.noaa.gov${o.href}" target="_blank">${o.text}</a>`
        ).join('<br/>')
      }},
      
      {data:'location', name:'location', title:'location of interview', render:data=>{
        return `<a href="https://web.archive.org/web/2/https://voices.nmfs.noaa.gov${data.href}" target="_blank">${data.text}</a>`
      }},

      {data:'description.text', name:'description', title:'description'},
      
      {data:'collection', name:'collection', title:'collection name', render:data=>{
        return `<a href="https://web.archive.org/web/2/https://voices.nmfs.noaa.gov${data.href}" target="_blank">${data.text}</a>`
      }},

    ]
  });
});