
object { glider( mononokeDecal )  translate <-16.0,8.0,8.0> }
object { glider( fanDecal )  translate <-44.0,8.0,8.0> }
object { glider( sanDecal )  translate <-72.0,8.0,8.0> }
object { glider( glitzDecal )  translate <-100.0,8.0,8.0> }
object { glider( ringDecal )  translate <-128.0,8.0,8.0> }
object { glider( goudDecal )  translate <-156.0,8.0,8.0> }
object { truck( lichtBruineVerf, TankLading ) rotate y*180.0 translate <100.0,0.0,120.0> }
object { truck( lichtBruineVerf, TankLading ) rotate y*180.0 translate <180.0,0.0,12.0> }
object { truck( lichtBruineVerf, kratFrame ) rotate y*180.0 translate <200.0,0.0,120.0> }
object { truck( lichtBruineVerf, TankLading ) rotate y*180.0 translate <280.0,0.0,120.0> }
object { truck( donkerBruineVerf, TankLading ) rotate y*180.0 translate <240.0,0.0,120.0> }
object { whale rotate y*0.0 translate <160.0,36.0,20.0> }
object { truck( lichtBruineVerf, Leeg ) translate <100.0,0.0,120.0> }
object { VolleFruitKrat  translate <102.6,2.7,120.0> }
// target cameraTarget is at [-44.0,8.0,8.0] roll 0.0 yaw 0.0 pitch 0.0
camera { location <72.0,32.0,64.0> direction 1.5*z right image_width/image_height*x look_at <-44.0,8.0,8.0> }
object { Kamer } object { Kamer translate <-(KamerLengte+WandDikte),0,0> } object { Kamer translate < KamerLengte+WandDikte,0,0> } object { Kamer translate <0,0,-(KamerDiepte+WandDikte)> } object { Kamer translate <0,0, (KamerDiepte+WandDikte)> } 
