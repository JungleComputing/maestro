
object { glider( mononokeDecal )  translate <-12.463999999999997,8.0,8.0> }
object { glider( fanDecal )  translate <-40.46400000000002,8.0,8.0> }
object { glider( sanDecal )  translate <-68.46399999999993,8.0,8.0> }
object { glider( glitzDecal )  translate <-96.46399999999993,8.0,8.0> }
object { glider( ringDecal )  translate <-124.46399999999993,8.0,8.0> }
object { glider( goudDecal )  translate <-152.4640000000001,8.0,8.0> }
object { truck( lichtBruineVerf, TankLading ) rotate y*180.0 translate <98.43999999999994,0.0,120.0> }
object { truck( lichtBruineVerf, TankLading ) rotate y*144.0 translate <180.0,0.0,12.0> }
object { truck( lichtBruineVerf, kratFrame ) rotate y*180.0 translate <198.43999999999994,0.0,120.0> }
object { truck( lichtBruineVerf, TankLading ) rotate y*180.0 translate <278.43999999999994,0.0,120.0> }
object { truck( donkerBruineVerf, TankLading ) rotate y*180.0 translate <238.43999999999994,0.0,120.0> }
object { whale rotate y*0.0 translate <160.0,36.0,20.0> }
object { truck( lichtBruineVerf, Leeg ) translate <100.0,0.0,120.0> }
object { VolleFruitKrat  translate <102.6,2.7,120.0> }
// target cameraTarget is at [-40.46400000000002,8.0,8.0] roll 0.0 yaw 0.0 pitch 0.0
camera { location <72.0,32.0,64.0> direction 1.5*z right image_width/image_height*x look_at <-40.46400000000002,8.0,8.0> }
object { Kamer } object { Kamer translate <-(KamerLengte+WandDikte),0,0> } object { Kamer translate < KamerLengte+WandDikte,0,0> } object { Kamer translate <0,0,-(KamerDiepte+WandDikte)> } object { Kamer translate <0,0, (KamerDiepte+WandDikte)> } 