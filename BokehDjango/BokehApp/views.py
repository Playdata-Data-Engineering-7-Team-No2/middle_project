from django.shortcuts import render
from django.http import HttpResponse
from bokeh.plotting import figure
from bokeh.embed import components


import BokehApp.namePickVis as namemap 
import BokehApp.hourPickVis  as hourmap
import BokehApp.dayPickVis  as daymap 
import BokehApp.seamapVis  as seamap 
import BokehApp.wmapVis  as weathermap 



def home(request):

    nMap = namemap.main() # 지명별 시각화
    hMap = hourmap.main() # 시간별 시각화
    dMap = daymap.main()  # 일별 시각화 
    jejuMap = weathermap.main()  # 제주도 맵 시각화 
    seaMap = seamap.main()  # 제주도 맵 시각화 

    nscript, ndiv = components(nMap)
    hscript, hdiv = components(hMap)
    dscript, ddiv = components(dMap)
    jscript, jdiv = components(jejuMap)
    sscript, sdiv = components(seaMap)
    
    return render(request, 'base.html', {'nscript': nscript, 'ndiv': ndiv,
                                          'hscript': hscript, 'hdiv': hdiv,
                                          'dscript': dscript, 'ddiv': ddiv,
                                          'jscript': jscript, 'jdiv': jdiv ,
                                          'sscript': sscript, 'sdiv': sdiv })

def hour(request):

    nMap = namemap.main() # 지명별 시각화
    hMap = hourmap.main() # 시간별 시각화
    dMap = daymap.main()  # 일별 시각화 
    jejuMap = weathermap.main()  # 제주도 맵 시각화 
    seaMap = seamap.main()  # 제주도 맵 시각화 

    nscript, ndiv = components(nMap)
    hscript, hdiv = components(hMap)
    dscript, ddiv = components(dMap)
    jscript, jdiv = components(jejuMap)
    sscript, sdiv = components(seaMap)
    
    return render(request, 'hourPick.html', {'nscript': nscript, 'ndiv': ndiv,
                                          'hscript': hscript, 'hdiv': hdiv,
                                          'dscript': dscript, 'ddiv': ddiv,
                                          'jscript': jscript, 'jdiv': jdiv ,
                                          'sscript': sscript, 'sdiv': sdiv })

def name(request):

    nMap = namemap.main() # 지명별 시각화
    hMap = hourmap.main() # 시간별 시각화
    dMap = daymap.main()  # 일별 시각화 
    jejuMap = weathermap.main()  # 제주도 맵 시각화 
    seaMap = seamap.main()  # 제주도 맵 시각화 

    nscript, ndiv = components(nMap)
    hscript, hdiv = components(hMap)
    dscript, ddiv = components(dMap)
    jscript, jdiv = components(jejuMap)
    sscript, sdiv = components(seaMap)
    
    return render(request, 'namePick.html', {'nscript': nscript, 'ndiv': ndiv,
                                          'hscript': hscript, 'hdiv': hdiv,
                                          'dscript': dscript, 'ddiv': ddiv,
                                          'jscript': jscript, 'jdiv': jdiv ,
                                          'sscript': sscript, 'sdiv': sdiv })

def day(request):

    nMap = namemap.main() # 지명별 시각화
    hMap = hourmap.main() # 시간별 시각화
    dMap = daymap.main()  # 일별 시각화 
    jejuMap = weathermap.main()  # 제주도 맵 시각화 
    seaMap = seamap.main()  # 제주도 맵 시각화 

    nscript, ndiv = components(nMap)
    hscript, hdiv = components(hMap)
    dscript, ddiv = components(dMap)
    jscript, jdiv = components(jejuMap)
    sscript, sdiv = components(seaMap)
    
    return render(request, 'dayPick.html', {'nscript': nscript, 'ndiv': ndiv,
                                          'hscript': hscript, 'hdiv': hdiv,
                                          'dscript': dscript, 'ddiv': ddiv,
                                          'jscript': jscript, 'jdiv': jdiv ,
                                          'sscript': sscript, 'sdiv': sdiv })