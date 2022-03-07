var new_data = [{
  word: 'play',
  scores: [{
    id: '1',
    score: 0.1
  }, {
    id: '2',
    score: 0.2
  },
  {
  	id:'5',
    score:0.5
  }]
},
{
  word: 'riverside',
  scores: [{
    id: '1',
    score: 0.2
  }, {
    id: '2',
    score: 0.3
  }]
},
{
	word: 'ground',
  scores: [{
    id: '1',
    score: 0.2
  }, {
    id: '3',
    score: 0.3
  }]
}
];

//BM25 score
function BM25_score(query,docId,data){

var k1=1.2;
var k2=100;
var b=0.75;
var avLen=3094; //average number of words in all documents
var totalFiles= 12521;
var totalWords= 38755637;
var K= k1*((1-b) + b* (0.9));
const words = query.split(" ");
//console.log(words);

var bm25=0.0;
for (var i=0; i < words.length; i++) {
   //console.log(words[i]);
   var word=words[i];
   let score_array = data.find(element => element.word == words[i]).scores;
   //console.log(scoress);
   let docObject=score_array.find(element => element.id == docId);
   var tfIdf=0.0;
   if (typeof(docObject) != "undefined"){
   	 tfIdf=docObject.score;
   }
   //console.log(tfIdf);
   let idf=totalFiles/score_array.length;
   let n_i=score_array.length ;
   //console.log(n_i);
   let f_i=Math.ceil(tfIdf/idf);
   //finding qf_i
   var qf_i=0;
   for (var j=0; j < words.length; j++){
   		if(words[j]==word){
      	qf_i=qf_i+1;
      }
   }
   //console.log(qf_i);

   bm25=bm25+ Math.log((totalFiles-n_i+0.5)/(n_i+0.5)) * (((k1+1)*f_i)/ (K+f_i)) * (((k2+1)*qf_i)/(k2+qf_i));
}
return bm25;
}

function Jelinec_Mercer_Smoothing(query,docId,data){
	var lambda=0.1;
  var document_length=3095*0.9; //need total number of words of docId
  var totalWordsAllDocs=38755637;
  var totalFiles= 12521;
  const words = query.split(" ");
  //console.log(words);

var JmScore=0.0;
for (var i=0; i < words.length; i++) {
   //console.log(words[i]);
   var word=words[i];
   let score_array = data.find(element => element.word == words[i]).scores;
   //console.log(scoress);
   let docObject=score_array.find(element => element.id == docId);
   var tfIdf=0.0;
   if (typeof(docObject) != "undefined"){
   	 tfIdf=docObject.score;
   }
   
   var idf=totalFiles/score_array.length;
   //console.log(n_i);
   let fq_d=Math.ceil(tfIdf/idf);
   //finding qf_i
   //var qf_i=0;
   
   var cq_i=0.0;
   for (var j=0; j < score_array.length; j++){
      	cq_i=cq_i+ Math.ceil(score_array[j].score/idf);
   }
   //console.log(qf_i);

   JmScore=JmScore+ Math.log((1-lambda)*(fq_d/document_length) + lambda*(cq_i/totalWordsAllDocs));
}
return JmScore;
  
}

function Dirichlet_Smoothing(query,docId,data){
	var meu=2000;
  var document_length=3095*0.9; //need total number of words of docId
  var totalWordsAllDocs=38755637;
  var totalFiles= 12521;
  const words = query.split(" ");
  //console.log(words);

var dirichletScore=0.0;
for (var i=0; i < words.length; i++) {
   //console.log(words[i]);
   var word=words[i];
   let score_array = data.find(element => element.word == words[i]).scores;
   //console.log(scoress);
   let docObject=score_array.find(element => element.id == docId);
   var tfIdf=0.0;
   if (typeof(docObject) != "undefined"){
   	 tfIdf=docObject.score;
   }
  
   var idf=totalFiles/score_array.length;
   //console.log(n_i);
   let fq_d=Math.ceil(tfIdf/idf);
   //finding qf_i
   //var qf_i=0;
   
   var cq_i=0.0;
   for (var j=0; j < score_array.length; j++){
      	cq_i=cq_i+ Math.ceil(score_array[j].score/idf);
   }
   //console.log(qf_i);

   dirichletScore=dirichletScore+ Math.log((fq_d+meu*(cq_i/totalWordsAllDocs))/(document_length+meu));
}
return dirichletScore;
  
}

console.log(BM25_score('play riverside','1',new_data));
console.log(Jelinec_Mercer_Smoothing('play riverside','3',new_data));
console.log(Dirichlet_Smoothing('play riverside','3',new_data));

//