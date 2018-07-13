function getOriginDeviceIDs(fileName) {
  let file = cat(fileName)  // read the file
  let originDeviceIDs = file.split('\n') // create an array of words
  originDeviceIDs = originDeviceIDs.filter( function(n){
    return !( n === undefined || typeof n !== 'string' || n.trim() === '')
  })
  return originDeviceIDs
}