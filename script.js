let ts0 = +(new Date())

$(document).ready(function(){
  $('#erica').DataTable({
    initComplete:()=>{
      let ts1 = +(new Date())
      console.log(`loaded in ${(ts1-ts0)/1000}s`)
    },
    ajax: '/data/fulltext-eric-records-lite.json',
    order: {
      name:'publicationdateyear',
      dir:'desc'
    },
    columns: [
      
      {data:'id', name:'id', title:'id', render:data=>{
        if(data.match(/^E[DJ][0-9]{6,7}$/)) {
          return `<a href="https://web.archive.org/web/2/https://files.eric.ed.gov/fulltext/${data}.pdf" target="_blank">${data}</a>`
        } else {
          return data
        }
      }},
      {data:'title', name:'title', title:'title'},
      {data:'author[</br></br>]', name:'author', title:'author'},
      {data:'publicationdateyear', name:'publicationdateyear', title:'year'}
      
    ]
  });
});