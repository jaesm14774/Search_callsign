#預防亂碼干擾，使整個排程中斷(flightaware的反擊XD)

source('C:/Users/jaesm14774/Desktop/compensate_fun.R',encoding = 'UTF-8')

if(length(n) !=0){
  Update_all(n)
}else print('Done')

