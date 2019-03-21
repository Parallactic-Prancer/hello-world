let name = process.argv[2];
if(!name) {
    name = 'Stranger';
}
console.log(`Hello ${name}.`);
console.log(`See you later, ${name}.`);