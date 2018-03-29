'''
Created on 2017. 6. 22.

@author: Holly
'''

if __name__ == '__main__':
    import com.wisenut.dmap_analyzer as anal
   
    sentence= "국내도로 달리는 중국차 올들어 되레 178대 늘어 켄보600 2월에만 73대 등록 중국의 사드(THAAD·고고도미사일방어체계) 배치 보복이 심화되는 가운데서도 가격경쟁력을 앞세운 중국산 자동차가 올 들어 2개월 만에 170여 대 이상 증가한 것으로 나타났다." 
    print(anal.get_emotion_type('a6960cb95381c3a54c946bb2c71ecb6c', sentence))