
//#declare Radiosity = 1;

global_settings {
    assumed_gamma 1.0
    noise_generator 3
#ifdef( Radiosity )
    radiosity {
      pretrace_start 0.08           // start pretrace at this size
      pretrace_end   0.04           // end pretrace at this size
      count 35                      // higher -> higher quality (1..1600) [35]
      nearest_count 5               // higher -> higher quality (1..10) [5]
      error_bound 1.8               // higher -> smoother, less accurate [1.8]
      recursion_limit 3             // how much interreflections are calculated (1..5+) [3]
      low_error_factor .5           // reduce error_bound during last pretrace step
      gray_threshold 0.0            // increase for weakening colors (0..1) [0]
      minimum_reuse 0.015           // reuse of old radiosity samples [0.015]
      brightness 0.6                  // brightness of radiosity effects (0..1) [1]

      adc_bailout 0.01/2
      //normal on                   // take surface normals into account [off]
      //media on                    // take media into account [off]
      //save_file "file_name"       // save radiosity data
      //load_file "file_name"       // load saved radiosity data
      //always_sample off           // turn sampling in final trace off [on]
      //max_sample 1.0              // maximum brightness of samples
    }
#end
}
// File: $Id: kamer.inc,v 1.9 2005/05/24 20:57:20 reeuwijk Exp $

#declare KamerEps = 0.00001;

#declare KamerHoogte = 64;
#declare KamerLengte = 192;
#declare KamerDiepte = 128;
#declare WandDikte = 16;
#declare TunnelBreedte = 16;
#declare TunnelHoogte = 16;

// Een gat in de kamer, wordt altijd negatief in csg gebruikt.
#declare Tunnel = box {
    <-(WandDikte+KamerEps),KamerEps,KamerEps >
    <KamerEps,TunnelHoogte,TunnelBreedte>
}

#declare DwarsTunnel = box {
    <KamerEps,     KamerEps,   -(WandDikte+KamerEps)>
    <TunnelBreedte,TunnelHoogte,KamerEps>
}

#declare Tunnels = union {
    object { Tunnel }
    object { Tunnel translate <WandDikte+KamerLengte,0,0> }
    object { Tunnel translate <                    0,0,KamerDiepte-TunnelBreedte> }
    object { Tunnel translate <WandDikte+KamerLengte,0,KamerDiepte-TunnelBreedte> }
    object { DwarsTunnel }
    object { DwarsTunnel translate <KamerLengte-TunnelBreedte,0,0> }
    object { DwarsTunnel translate <                        0,0,WandDikte+KamerDiepte> }
    object { DwarsTunnel translate <KamerLengte-TunnelBreedte,0,WandDikte+KamerDiepte> }
}

#declare KamerLamp = light_source {
    0*x // light's position (translated below)
    color rgb 1  // light's color
#if(0)
    area_light
    <1, 0, 0> <0, 0, 1> // lights spread out across this distance (x * z)
    5, 5                // total number of lights in grid (4x*4z = 16 lights)
    adaptive 2          // 0,1,2,3... 
#end
    fade_distance 10
}

#declare KamerPigment = pigment { color 0.85*<1,1,1> };

#declare Kamer = union {
    difference {
        box { <-WandDikte,-WandDikte,-WandDikte> <KamerLengte+ WandDikte,KamerHoogte+WandDikte,KamerDiepte+WandDikte> }
        box { <0, 0, 0> <KamerLengte,KamerHoogte,KamerDiepte> }
        object { Tunnels }
        pigment { KamerPigment }
    }
    object {
        KamerLamp
        translate <KamerLengte/2, KamerHoogte-1, KamerDiepte/2>
    }
}

// File: $Id: actors.inc,v 1.36 2005/07/07 08:40:52 reeuwijk Exp $

#declare ActorsEps = 0.0001;

#declare chroomFinish = finish {
    reflection { rgb 0.6 }
    diffuse 0.6
    brilliance 1.0
    specular 1.0
    roughness 0.05
    metallic 1.0
}

#declare Chroom = material {
    texture {
        pigment {
            rgb <1, 1, 1>
        }
        normal {
            agate 0.05
            slope_map {
                [ 0.000000, <0.000000, 0.000000> ]
                [ 1.000000, <1.000000, 0.000000> ]
            }
            accuracy 0.01
        }
        finish { chroomFinish }
    }
}

#declare verfFinish = finish {
    reflection { rgb 0.05 }
    diffuse 0.6
    brilliance 1.0
    specular 1.0
    roughness 0.05
    metallic 1.0
}

#declare verfNormal = normal {
    bozo 0.15
    scale 0.8
    turbulence 0.15
}

#declare lichtBruin = rgb <0.8, 0.4, 0.13>;

#declare goudVerf = material {
    texture {
        pigment { rgb <1,0.8,0.3> }
        finish { verfFinish }
        normal { verfNormal }
    }
}

#declare goudChroomVerf = material {
    texture {
        pigment { rgb <1,0.8,0.3> }
        finish { chroomFinish }
   }
}

#declare witteVerf = material {
    texture {
        pigment { rgb <1,1,1> }
        finish { verfFinish }
        normal { verfNormal }
    }
}

#declare rodeVerf = material {
    texture {
        pigment { rgb <1,0,0> }
        finish { verfFinish }
        normal { verfNormal }
    }
}

#declare groeneVerf = material {
    texture {
        pigment { rgb <0.4,1,0.4> }
        finish { verfFinish }
        normal { verfNormal }
    }
}

#declare blauweVerf = material {
    texture {
        pigment { rgb <0,0,1> }
        finish { verfFinish }
        normal { verfNormal }
    }
}

#declare donkerBruin = rgb <0.25, 0.1, 0.08>;

#declare donkerBruineVerf = material {
    texture {
        pigment { color donkerBruin }
        finish { verfFinish }
        normal { verfNormal }
    }
}

#declare lichtBruineVerf = material {
    texture {
        pigment { color lichtBruin }
        finish { verfFinish }
        normal { verfNormal }
    }
}

#declare veren = material {
    texture {
        pigment {
            cells
            color_map {
                [ 0.000000 color  donkerBruin  ]
                [ 1.000000 color  lichtBruin  ]
            }
            rotate <0.0, 0.0, -45>
            scale 0.55*<0.3, 0.1, 0.1>
            turbulence 0.4
        }
    }
}

#declare gliderHuidoud = material {
    texture {
        pigment { color <1,0.90,0.85> }
        finish { phong 0.3 roughness 1 }
    }
}

#declare gliderHuid = material {
    texture {
        pigment {
            bozo
            color_map {
                [ 0.000000 rgb 1.2*<0.587594, 0.551537, 0.508248> ]
                [ 0.500000 rgb 1.1*<0.784665, 0.727001, 0.686122> ]
                [ 1.000000 rgb <0.864958, 0.822538, 0.779736> ]
            }
            turbulence 0.5
            scale 0.06
        }
        normal {
            ripples 0.3
            turbulence 0.5
        }
        finish {
            phong 0.3
            phong_size 20
            roughness 1
        }
    }
}

#declare gliderTailHalf = prism {                                
    linear_sweep  // or conic_sweep for tapering to a point
    linear_spline // linear_spline | quadratic_spline | cubic_spline | bezier_spline 
    -0.05,         // height 1
     0.05,         // height 2
    5,           // number of points
    // (--- the <u,v> points ---)
    < -0.4, -0.8>,
    < -0.1, 0>, < 0.6, 0>, < -0.1, -0.7>,
    < -0.4, -0.8>
    // , <0.2,   0.2> // match 2nd point, if quadratic_spline add this
    // , <0.2,  -1.0> // match 1st point, if cubic_spline add this as well as the other
    // [open]
    // [sturm]
    material { veren scale 0.8 }
}


#declare gliderWing = prism {                                
    linear_sweep  // or conic_sweep for tapering to a point
    linear_spline // linear_spline | quadratic_spline | cubic_spline | bezier_spline 
    -0.05,         // height 1
     0.05,         // height 2
    5,           // number of points
    // (--- the <u,v> points ---)
    < -1.2, -0.96>,
    < -0.2,  -0.25>,
    < 0.7, -0.25>,
    < -0.7, -0.90>,
    < -1.2, -0.96>
    // , <0.2,   0.2> // match 2nd point, if quadratic_spline add this
    // , <0.2,  -1.0> // match 1st point, if cubic_spline add this as well as the other
    // [open]
    // [sturm]
    material { veren }
}

#declare gliderFin = prism {                                
    linear_sweep  // or conic_sweep for tapering to a point
    linear_spline // linear_spline | quadratic_spline | cubic_spline | bezier_spline 
    -0.04,         // height 1
     0.04,         // height 2
    5,           // number of points
    // (--- the <u,v> points ---)
    < -0.3, -0.5>,
    < 0,  -0.20>,
    < 0.6, -0.25>,
    < -0, -0.5>,
    < -0.3, -0.5>
    // , <0.2,   0.2> // match 2nd point, if quadratic_spline add this
    // , <0.2,  -1.0> // match 1st point, if cubic_spline add this as well as the other
    // [open]
    // [sturm]
    rotate 90*x
    material { donkerBruineVerf }
}

#macro Side( A, B, C, D )
    union {
        triangle { A, B, C }
        triangle { B, C, D }
    }
#end

// A wegde from x1 to x2 with linearly varying square cross-section size1 to size2
#macro Wedge( x1, size1, x2, size2 )
    #local P11 = <x1, size1/2, size2/2>;
    #local P12 = <x1, size1/2,-size2/2>;
    #local P13 = <x1,-size1/2,-size2/2>;
    #local P14 = <x1,-size1/2, size2/2>;
    #local P21 = <x2, size2/2, size2/2>;
    #local P22 = <x2, size2/2,-size2/2>;
    #local P23 = <x2,-size2/2,-size2/2>;
    #local P24 = <x2,-size2/2, size2/2>;
    union {
        Side( P11, P12, P21, P22 )
        Side( P12, P13, P22, P23 )
        Side( P13, P14, P23, P24 )
        Side( P14, P11, P24, P21 )
        Side( P11, P12, P13, P14 )
        Side( P21, P22, P23, P24 )
    }
#end

#declare gliderBody = union {
    Wedge( -1.6, 0.25, 1.6, 0.6 )
    cylinder { <1.6,0,-0.3> <1.6,0,0.3> 0.3 }
    material { gliderHuid }
}

#declare oogMaterial = material {
    texture {
        pigment {
            rgbt <1, 0.5, 0.5, 0.5>
        }
        normal {
            bumps 0.5
            accuracy 0.01
        }
        finish {
            reflection {
                rgb <0.0, 0.0, 0.0>
            }
            brilliance 1.0
            phong 1.0
            phong_size 20
            roughness 0.5
            conserve_energy
        }
    }
    interior {
        ior 1.6
        dispersion 1.0
        dispersion_samples 7
    }
}

#declare gliderOog = sphere {
    <0,0,0> 0.1
    material { goudChroomVerf }
}

#macro glider( decal )
union {
    object { gliderWing rotate z*3 translate y*0.1 }
    object { gliderWing scale <1,1,-1> rotate z*3 translate y*0.1 }
    object { gliderTailHalf translate x*-1.6 }
    object { gliderTailHalf scale <1,1,-1> translate x*-1.6 }
    object { gliderFin translate <-0.2,0,0> }
    object { gliderBody }
    object { gliderOog translate <1.4,0.05,-0.28> }
    object { gliderOog scale <1,1,-1> translate <1.4,0.05,0.28> }
    object { decal translate <0.9,0,0.3> }
    object { decal scale <1,1,-1> translate <0.9,0,-0.3> }
    scale 4
}
#end

#declare glitzDecal = sphere {
    <0,0,0>, 0.2
    scale <1,1,0.2>
    texture {
        pigment { radial frequency 1 }
        finish {specular  0.3}
        rotate x*90
    }
}

#declare mononokeDecal = prism {
    linear_spline
    linear_sweep
    0.0,
    0.01,
    4,
    <0.02, -1.0>,
    <0.6, 1.0>,
    <-0.6, 1.05>,
    <0.02, -1.0>
    sturm
    scale 0.2
    rotate x*90
    rotate z*180
    material {
        texture {
            pigment {
                rgb <0.6, 0.2, 0.2>
            }
            normal {
                bozo 0.2
            }
        }
        scale <0.03, 0.03, 0.03>
    }
}

#declare ringDecal = difference {
    cylinder { <0,0,0.01> 0, 0.2 }
    cylinder { <0,0,0.02> <0,0,-0.01>, 0.1 }
    material { goudVerf }
}

#declare fanSpaakDikte = 0.012;
#declare fanStraal = 0.42;

#declare fanSpaak = cylinder {
    <-0.04,0,0> <0.3,0,0> fanSpaakDikte scale <1,1,0.5> 
    material {
        texture { pigment { rgb 0 } }
    }
}

#declare fanDecal = union {
    object { fanSpaak }
    object { fanSpaak rotate z*30 }
    object { fanSpaak rotate z*60 }
    object { fanSpaak rotate z*90 }
    object { fanSpaak rotate z*120 }
    object { fanSpaak rotate z*150 }

    difference {
        cylinder { <0,0,0.01> 0, fanStraal }
        cylinder { <0,0,0.02> <0,0,-0.01>, 0.5*fanStraal }
        box { <-0.5, fanSpaakDikte, -0.01> <1,1,0.03> rotate z*150 }
        box { <-0.5, -fanSpaakDikte, -0.01> <1,-1,0.03>  }
        material { goudVerf }
    }
    translate <-0.02,-0.2,0>
}

#declare goudDecal = sphere {
    <0,0,0>, 0.2
    scale <1,1,0.2>
    material { goudVerf }
}

#declare sanDecal = sphere {
    <0,0,0>, 0.2
    scale <1,1,0.2>
    material { witteVerf }
}

#declare redDecal = sphere {
    <0,0,0>, 0.2
    scale <1,1,0.2>
    material { rodeVerf }
}

#declare decal = object { ringDecal }

#declare groupShot = union {
    object { glider( redDecal ) rotate z*90 translate <8,3,14> }
    object { glider( mononokeDecal ) rotate z*90 translate <10,3,14> }
    object { glider( fanDecal ) rotate z*90 translate <12,3,14> }
    object { glider( goudDecal ) rotate z*90 translate <14,3,14> }
    object { glider( glitzDecal ) rotate z*90 translate <16,3,14> }
    object { glider( sanDecal ) rotate z*90 translate <18,3,14> }
}

#declare truckBreedte = 4;
#declare voorBalStraal = 0.8;
#declare naafStraal = 0.5*voorBalStraal;
#declare spoorBreedte = 0.8*truckBreedte;
#declare bandDikte = 0.55;
#declare truckHoogte = 0.9;
#declare truckDikte = 1.8;
#declare truckOverhang = 1.4;
#declare truckLengte = 2*truckBreedte;
#declare wielKastStraal = 1.2*voorBalStraal;
#declare oogHoogte = 0.8;
#declare oogBreedte = 0.46*truckBreedte;
#declare oogDiepte = 1.0;
#declare OogPositie = 0.7;
#declare truckWandDikte = 0.08;
#declare balAfstand = truckLengte-(truckOverhang+oogDiepte+voorBalStraal);
#declare laadSpleet = 0.03;

#declare laadLengte = truckLengte;
#declare laadBreedte = truckBreedte;

#declare zwartRubber = material { texture { pigment { rgb 0 } } }

#declare voorBal = sphere { <0,voorBalStraal,0> voorBalStraal material { zwartRubber } }

#declare wiel = union {
    difference {
        cylinder { <0,voorBalStraal,-bandDikte/2> <0,voorBalStraal,bandDikte/2>, voorBalStraal  }
        cylinder { <0,voorBalStraal,-(ActorsEps+bandDikte/2)> <0,voorBalStraal,ActorsEps+bandDikte/2>, naafStraal  }
        material { zwartRubber }
    }
    cylinder { <0,voorBalStraal,-bandDikte/4> <0,voorBalStraal,bandDikte/4>, (naafStraal+voorBalStraal )/2 material { Chroom } }
}


#declare frameOog = box {
   <-oogDiepte,ActorsEps,-oogBreedte>
   <ActorsEps,oogHoogte,ActorsEps>
}

#macro frame( mat )
union {
   difference {
        box { 0, <truckLengte,truckDikte,truckBreedte>  }
        object { frameOog translate <truckLengte, OogPositie, truckBreedte> }
        object { frameOog scale <1,1,-1>  translate <truckLengte, OogPositie, 0> }
        material { mat }
    }
    box {
       <truckLengte-(oogDiepte+ActorsEps),ActorsEps,ActorsEps>
       <truckLengte-truckWandDikte,truckDikte-ActorsEps,truckBreedte-ActorsEps>
       material { goudChroomVerf }
    }
    translate <-truckOverhang,truckHoogte,-truckBreedte/2> 
}
#end

#macro truck( mat, lading )
union {
    object { voorBal translate <balAfstand,0,0> }
    object { wiel translate <0,0,spoorBreedte/2> }
    object { wiel translate <0,0,-spoorBreedte/2> }
    object { frame( mat ) }
    object { lading translate <laadLengte/2-truckOverhang,truckHoogte+truckDikte+laadSpleet,laadBreedte/2-truckBreedte/2> }
}
#end

#declare Leeg = box { 0, 0 texture { pigment { color <0,0,0,0,0> } } }

#macro Tank( lengte, straal )
union {
    #local squash = 0.5;
    #local i = squash*straal;
    cylinder { <i-ActorsEps,0,0> <lengte+ActorsEps-i,0,0>, straal }
    sphere { 0 straal scale <squash,1,1> translate <i,0,0>} 
    sphere { 0 straal scale <squash,1,1> translate <lengte-i,0,0>} 
}
#end

#declare TankLading =
union {
    #local s = laadBreedte/2;
    object{ Tank( laadLengte, s ) translate <-laadLengte/2,s,0> }
    #local w = 0.1;
    box { <0,0,-laadBreedte/2>, <w,s,laadBreedte/2> translate < laadLengte/4,0,0> }
    box { <0,0,-laadBreedte/2>, <w,s,laadBreedte/2> translate <-laadLengte/4,0,0> }
    material { witteVerf }
}

#declare whaleStraal = 6;
#declare whaleLengte = 3*whaleStraal;
#declare gondelStraal = whaleStraal*0.35;
#declare gondelLengte = whaleLengte*0.5;
#declare gondelDiepte = 1.0;
#declare gondelSquash = 1.0;
#declare whaleVinDikte = 0.06*whaleStraal;
#declare whaleVinLengte = 0.5*whaleLengte;
#declare whaleVinBreedte = 0.3*whaleStraal;
#declare whaleVinHoogte = 0.85*whaleStraal;
#declare whaleAntenneLengte = 0.3*whaleStraal;
#declare whaleAntenneStraal = 0.025*whaleStraal;
#declare whaleAntenneBusStraal = 2*whaleAntenneStraal;
#declare whaleAntenneBusLengte = 0.2*whaleAntenneLengte;

#declare gondel = union {
    difference {
        union {
            cylinder { <0,0,0> <0,whaleStraal,0>, gondelStraal scale <gondelSquash,1,1> translate x*gondelLengte/2 }
            cylinder { <0,0,0> <0,whaleStraal,0>, gondelStraal scale <gondelSquash,1,1> translate x*-gondelLengte/2 }
            box {
                <-gondelLengte/2,           0, -gondelStraal>
                < gondelLengte/2, whaleStraal,  gondelStraal>
            }
        }
        box {
            <-gondelSquash*1.1*gondelStraal,0.3*gondelDiepte,-1.1*gondelStraal>
            < gondelSquash*1.1*gondelStraal,0.8*gondelDiepte, 1.1*gondelStraal>
            translate x*gondelLengte/2
        }
        material { lichtBruineVerf }
    }
    cylinder {
        <0,ActorsEps,0>
        <0,whaleStraal,0>,
        0.9*gondelStraal scale <gondelSquash,1,1> translate x*gondelLengte/2
        material { goudChroomVerf }
    }
}

#declare whaleVin = union {
    box {
        <              0,-whaleVinDikte/2,-whaleVinBreedte/2>
        <-whaleVinLengte, whaleVinDikte/2, whaleVinBreedte/2>
    }
    cylinder {
        <0,-whaleVinDikte/2,0>
        <0, whaleVinDikte/2,0>
        whaleVinBreedte/2
    }
    material { lichtBruineVerf }
}

#declare whaleAntenne = union {
    cylinder {
        <0,0,0> <whaleAntenneLengte+whaleAntenneBusLengte/2,0,0>  whaleAntenneStraal
    }
    cylinder {
        <whaleAntenneLengte,0,0> <whaleAntenneLengte+whaleAntenneBusLengte,0,0>  whaleAntenneBusStraal
    }
    material { donkerBruineVerf }
}

#declare whale = union {
    union {
        union {
            sphere { <whaleLengte/2,whaleStraal,0>, whaleStraal }
            sphere { <-whaleLengte/2,whaleStraal,0>, whaleStraal }
            cylinder { <-whaleLengte/2,whaleStraal,0> <whaleLengte/2,whaleStraal,0>, whaleStraal }
            texture {
                pigment { color rgb <1,0.85,0.72> }
                normal { wrinkles 0.12 scale 0.1 }
                finish { specular 0.2 }
            }
        }
        object { whaleVin translate <whaleVinLengte/2,whaleVinHoogte,whaleStraal> }
        object { whaleVin scale <1,1,-1> translate <whaleVinLengte/2,whaleVinHoogte,-whaleStraal> }
        object { whaleAntenne translate <whaleLengte/2+whaleStraal,whaleStraal,0> }
        translate y*gondelDiepte
    }
    //cylinder { <0,-30,0> <0,30,0> 0.2 material { rodeVerf } }
    object { gondel }
}

#declare ping = light_source {
    <0,0,0> color rgb 0.1
    looks_like {
        sphere {
            <0,0,0>, 0.1
            pigment { color rgb <1,1,1> }
            finish { ambient 1 }
        }
    }
}

#declare bak = difference {
    #local w = laadBreedte/20;
    #local h = 2;

    box { < -laadLengte/2,0, -laadBreedte/2>, <laadLengte/2  ,h  ,laadBreedte/2> }
    box { <w-laadLengte/2,w,w-laadBreedte/2>, <laadLengte/2-w,h+w,laadBreedte/2-w> }
    material { goudVerf }
}

#declare kratHout = material {
    texture {
        pigment {
            rgb <1, 0.85, 0.7>
        }
        normal {
            wrinkles 0.1
            scale <0.1, 0.1, 0.1>
            turbulence <0.2, 0.2, 0.2>
        }
    }
}

#declare blokHout = material {
    texture {
        pigment {
            rgb <0.5, 0.425, 0.35>
        }
        normal {
            wrinkles 0.1
            scale <0.1, 0.1, 0.1>
            turbulence <0.2, 0.2, 0.2>
        }
    }
}

#declare kratLengte = laadLengte;
#declare kratDiepte = laadBreedte;
#declare kratHoogte = kratDiepte;
#declare kratBlokLengte = 0.5;
#declare kratSpeling = kratBlokLengte/3;

#declare blok = box {
    <0,0,0> <kratBlokLengte,kratBlokLengte,kratBlokLengte >
    material { blokHout }
}

#declare kratBlokken = union {
    object { blok translate <-kratLengte/2, 0, -kratDiepte/2> }
    object { blok translate <kratLengte/2-kratBlokLengte, kratHoogte-kratBlokLengte,-kratDiepte/2> }
    object { blok translate <kratLengte/2-kratBlokLengte, 0                        ,-kratDiepte/2> }
    object { blok translate <-kratLengte/2            , kratHoogte-kratBlokLengte,-kratDiepte/2> }
    object { blok translate <kratLengte/2-kratBlokLengte, 0                        , kratDiepte/2-kratBlokLengte> }
    object { blok translate <-kratLengte/2            , 0                        , kratDiepte/2-kratBlokLengte> }
    object { blok translate <kratLengte/2-kratBlokLengte, kratHoogte-kratBlokLengte, kratDiepte/2-kratBlokLengte> }
    object { blok translate <-kratLengte/2              , kratHoogte-kratBlokLengte, kratDiepte/2-kratBlokLengte> }
}

#declare krat = union {
    box {
        <kratSpeling-kratLengte/2,kratSpeling,kratSpeling-kratDiepte/2 > <kratLengte/2-kratSpeling,kratHoogte-kratSpeling,kratDiepte/2-kratSpeling >
        material { kratHout }
    }
    object { kratBlokken }
}

#declare frameDikte = 0.3;

#declare kratFrame = union {
    difference {
        box {
            <kratSpeling           -kratLengte/2,kratSpeling           ,kratSpeling-kratDiepte/2>
            <kratLengte/2-kratSpeling,kratHoogte-kratSpeling,kratDiepte/2-kratSpeling >
        }
        box {
            <kratSpeling+frameDikte-kratLengte/2,kratSpeling+frameDikte,           -kratDiepte/2>
            <kratLengte/2-(kratSpeling+frameDikte),kratHoogte-(kratSpeling+frameDikte),kratDiepte/2>
        }
        box {
            <kratSpeling+frameDikte-kratLengte/2,0                     ,kratSpeling+frameDikte-kratDiepte/2>
            <kratLengte/2-(kratSpeling+frameDikte),kratHoogte,kratDiepte/2-(kratSpeling+frameDikte)>
        }
        box {
            <                      -kratLengte/2,kratSpeling+frameDikte,kratSpeling+frameDikte-kratDiepte/2>
            <                       kratLengte/2,kratHoogte-(kratSpeling+frameDikte),kratDiepte/2-(kratSpeling+frameDikte)>
        }
        material { kratHout }
    }
    object { kratBlokken }
}

#declare cylinderFrame = union {
    object { kratFrame }
    union {
        object{ Tank( laadLengte-2*kratSpeling, (laadBreedte/2)-kratSpeling ) translate <kratSpeling-laadLengte/2,laadBreedte/2,0> }
        #local w = 0.1;
        box { <0,frameDikte,frameDikte-laadBreedte/2>, <w,laadBreedte-frameDikte,laadBreedte/2-frameDikte> translate x*laadLengte/4 }
        box { <0,frameDikte,frameDikte-laadBreedte/2>, <w,laadBreedte-frameDikte,laadBreedte/2-frameDikte> translate -x*laadLengte/4 }
        material { witteVerf }
    }
}

#declare bollenFrame = union {
    object { kratFrame }
    union {
        sphere {
            <( laadLengte/4)+kratSpeling,laadBreedte/2,0> (laadBreedte/2)-kratSpeling 
        }
        sphere {
            <(-laadLengte/4)-kratSpeling,laadBreedte/2,0> (laadBreedte/2)-kratSpeling 
        }
        #local w = 0.1;
        box { <0,frameDikte,frameDikte-laadBreedte/2>, <w,laadBreedte-frameDikte,laadBreedte/2-frameDikte> translate  x*laadLengte/4 }
        box { <0,frameDikte,frameDikte-laadBreedte/2>, <w,laadBreedte-frameDikte,laadBreedte/2-frameDikte> translate -x*laadLengte/4 }
        material { witteVerf }
    }
}

#declare rollenFrame = union {
    object { kratFrame }
    union {
        cylinder {
            <(laadLengte/4)+kratSpeling-frameDikte,kratSpeling,0>
            <(laadLengte/4)+kratSpeling-frameDikte,laadBreedte-(kratSpeling+frameDikte),0>
            (laadBreedte/2)-(kratSpeling+frameDikte)
        }
        cylinder {
            <(-laadLengte/4)+kratSpeling-frameDikte,kratSpeling,0>
            <(-laadLengte/4)+kratSpeling-frameDikte,laadBreedte-(kratSpeling+frameDikte),0>
            (laadBreedte/2)-(kratSpeling+frameDikte)
        }
        box { <-laadLengte/2+kratSpeling,kratSpeling,kratSpeling-laadBreedte/2> <laadLengte/2-kratSpeling,kratSpeling+0.01,laadBreedte/2-kratSpeling> }
        material { witteVerf }
    }
}

#declare bladBreedte = 0.3;
#declare bladLengte = 0.8;
#declare bladDikte = 0.05;

#declare blad = box {
    <0,-bladDikte/2,-bladBreedte/2>
    <bladLengte,bladDikte/2,bladBreedte/2>
    pigment { color <0.7,1,0> }
}

#declare bladHoek = 25;
#declare kransShift = 0.20;

#declare bladKrans = union {
    object { blad rotate z*bladHoek translate x*kransShift }
    object { blad rotate z*bladHoek translate x*kransShift rotate y*60  }
    object { blad rotate z*bladHoek translate x*kransShift rotate y*120  }
    object { blad rotate z*bladHoek translate x*kransShift rotate y*180 }
    object { blad rotate z*bladHoek translate x*kransShift rotate y*240  }
    object { blad rotate z*bladHoek translate x*kransShift rotate y*300  }
}

#declare bladStepScale = 1;

#declare bladKroon = union {
    object { bladKrans }
    object { bladKrans scale <bladStepScale,1,bladStepScale> translate y*bladDikte rotate y*20 }
    object { bladKrans scale <bladStepScale*bladStepScale,1,bladStepScale*bladStepScale> translate 2*y*bladDikte rotate y*40 }
    object { bladKrans scale <bladStepScale*bladStepScale,1,bladStepScale*bladStepScale> translate 3*y*bladDikte rotate y*60 }
}

#declare marker = union {
    #local r = 0.15;
    #local l = 15;
    cylinder { <-l,0,0> <l,0,0>, r pigment { color <1,0,0> } }
    cylinder { <0,-l,0> <0,l,0>, r pigment { color <0,1,0> } }
    cylinder { <0,0,-l> <0,0,l>, r pigment { color <0,0,1> } }
}

#declare sproeierSteun = union {
    #local steunStraal = 0.10;
    #local steunHoek = 30;
    #local steunLengte = laadBreedte/2;
    #local KlemStraal = 2*steunStraal;
    #local KlemBreedte = 5*steunStraal;
    cylinder { <0,0,-KlemBreedte/2> <0,0,KlemBreedte/2> KlemStraal }
    cylinder { <0,0,0> <0,-steunLengte,0> steunStraal rotate z*steunHoek }
    cylinder { <0,0,0> <0,-steunLengte,0> steunStraal rotate -z*steunHoek }
    material { witteVerf }
}

#declare sproeiers = union {
    #local tankStraal = laadBreedte/4;
    #local sproeiHoogte = laadBreedte/4;
    #local sproeiBreedte = 2.5*laadBreedte;
    #local sproeiBuisStraal = 0.1;
    #local tankPlaats = laadLengte/2-1.5*tankStraal;

    object { Tank( laadBreedte, tankStraal ) rotate y*90 translate <-tankPlaats,tankStraal,laadBreedte/2> material { witteVerf } }
    cylinder { <0,0,-sproeiBreedte> <0,0,sproeiBreedte> sproeiBuisStraal material { goudVerf } translate <0,sproeiHoogte,0> }
    object { Tank( laadBreedte, tankStraal ) rotate y*90 translate <tankPlaats,tankStraal,laadBreedte/2> material { witteVerf } }
    object { sproeierSteun translate <0,sproeiHoogte,laadBreedte/3> }
    object { sproeierSteun translate <0,sproeiHoogte,-laadBreedte/3> }
    object { sproeierSteun translate <tankPlaats,sproeiHoogte,laadBreedte/3> }
    object { sproeierSteun translate <tankPlaats,sproeiHoogte,-laadBreedte/3> }
    object { sproeierSteun translate <-tankPlaats,sproeiHoogte,laadBreedte/3> }
    object { sproeierSteun translate <-tankPlaats,sproeiHoogte,-laadBreedte/3> }
}

#declare FruitNormal = normal {
    wrinkles 0.4
    scale 0.2
}

#declare FruitFinish = finish {
    phong_size 10
    specular 0.25
    roughness 0.5
}

#declare RedGreenFruitMaterial = material {
    texture {
        pigment {
            rgbt <1.0, 0.5, 0, 0.4>
        }
        normal { FruitNormal }
        finish { FruitFinish }
    }
}

#declare RedBlueFruitMaterial = material {
	texture {
		pigment {
			rgbt <1.0, 0.0, 0.5, 0.4>
		}
		normal { FruitNormal }
		finish { FruitFinish }
	}
}

#declare GreenRedFruitMaterial = material {
	texture {
		pigment {
			rgbt <0.5, 1.0, 0, 0.4>
		}
		normal { FruitNormal }
		finish { FruitFinish }
	}
}

#declare GreenBlueFruitMaterial = material {
	texture {
		pigment {
			rgbt <0.0, 1.0, 0.0, 0.4>
		}
		normal { FruitNormal }
		finish { FruitFinish }
	}
}

#declare BlueGreenFruitMaterial = material {
	texture {
		pigment {
			rgbt <0.0, 0.5, 1.0, 0.4>
		}
		normal { FruitNormal }
		finish { FruitFinish }
	}
}

#declare BlueRedFruitMaterial = material {
	texture {
		pigment {
			rgbt <0.5, 0.0, 1.0, 0.4>
		}
		normal { FruitNormal }
		finish { FruitFinish }
	}
}


#declare Fruit = sphere { <0,1,0>, 1 material { GreenBlueFruitMaterial } }

#declare FruitKrat = union {
    #declare glasDikte = 0.05;
    #local r = (laadBreedte-2*(kratSpeling+frameDikte))/4;

    #local HoogteSpijl = box { <-frameDikte/2,kratSpeling,-frameDikte/2> <frameDikte/2,laadBreedte-kratSpeling,frameDikte/2> }

    object { kratFrame }
    box {
        <(kratSpeling+frameDikte/2)-laadLengte/2,kratSpeling+frameDikte/2-glasDikte/2,(kratSpeling+frameDikte/2)-laadBreedte/2>
        <laadLengte/2-(kratSpeling+frameDikte/2),kratSpeling+frameDikte/2+glasDikte/2,laadBreedte/2-(kratSpeling+frameDikte/2)>
        material { kratHout }
    }
    union {
        object { HoogteSpijl translate <r,0,laadBreedte/2-(frameDikte/2+kratSpeling)> }
        object { HoogteSpijl translate <r,0,frameDikte/2+kratSpeling-laadBreedte/2> }
        object { HoogteSpijl translate <-r,0,laadBreedte/2-(frameDikte/2+kratSpeling)> }
        object { HoogteSpijl translate <-r,0,frameDikte/2+kratSpeling-laadBreedte/2> }
        object { HoogteSpijl translate <3*r,0,laadBreedte/2-(frameDikte/2+kratSpeling)> }
        object { HoogteSpijl translate <3*r,0,frameDikte/2+kratSpeling-laadBreedte/2> }
        object { HoogteSpijl translate <-3*r,0,laadBreedte/2-(frameDikte/2+kratSpeling)> }
        object { HoogteSpijl translate <-3*r,0,frameDikte/2+kratSpeling-laadBreedte/2> }
        object { HoogteSpijl translate <laadLengte/2-(kratSpeling+frameDikte/2),0,r> }
        object { HoogteSpijl translate <laadLengte/2-(kratSpeling+frameDikte/2),0,-r> }
        object { HoogteSpijl translate <(kratSpeling+frameDikte/2)-laadLengte/2,0,r> }
        object { HoogteSpijl translate <(kratSpeling+frameDikte/2)-laadLengte/2,0,-r> }
        material { kratHout }
    }
}
#declare kratSpeling = 0.0;

#declare VolleFruitKrat = union {
    #local r = (laadBreedte-2*(kratSpeling+frameDikte))/4;

    object { FruitKrat }
    union {
        object { Fruit scale r translate <3*r,kratSpeling,-r> }    
        object { Fruit scale r translate <3*r,kratSpeling, r> }
        object { Fruit scale r translate <r,kratSpeling,-r> }    
        object { Fruit scale r translate <r,kratSpeling, r> }
        object { Fruit scale r translate <-r,kratSpeling,-r> }    
        object { Fruit scale r translate <-r,kratSpeling, r> }
        object { Fruit scale r translate <-3*r,kratSpeling,-r> }    
        object { Fruit scale r translate <-3*r,kratSpeling, r> }

        object { Fruit scale r translate <3*r,2*r+kratSpeling,-r> }    
        object { Fruit scale r translate <3*r,2*r+kratSpeling, r> }
        object { Fruit scale r translate <r,2*r+kratSpeling,-r> }    
        object { Fruit scale r translate <r,2*r+kratSpeling, r> }
        object { Fruit scale r translate <-r,2*r+kratSpeling,-r> }    
        object { Fruit scale r translate <-r,2*r+kratSpeling, r> }
        object { Fruit scale r translate <-3*r,2*r+kratSpeling,-r> }    
        object { Fruit scale r translate <-3*r,2*r+kratSpeling, r> }
        translate y*(kratSpeling+frameDikte/2)
    }
}

#declare liftPaalStraal=1;
#declare liftPaalAnkerDikte=0.2;
#declare liftPaalAnkerHoogte=WandDikte/2;
#declare liftPaalSpeling = 1.4;
#declare LiftPlatformSpeling = 3;
#declare LiftSchachtBreedte = 28;
#declare LiftPlatformBreedte = LiftSchachtBreedte-LiftPlatformSpeling;
#declare LiftGap = 0.3;
#declare LiftDikte = WandDikte/3;

#declare space=13;
#declare spread = 5;
#declare c1 = KamerDiepte/2-spread/2;
#declare c2 = KamerDiepte/2+spread/2;
#declare stopY = KamerLengte/2+LiftSchachtBreedte/2+10;

#declare liftPaalAnker = box {
    <-liftPaalAnkerDikte,-liftPaalAnkerHoogte/2,0>
    < liftPaalAnkerDikte, liftPaalAnkerHoogte/2,4*liftPaalSpeling>
    rotate y*45
    pigment { color <1,1,1> }
        //material { Chroom }   // Expensive!!
}

#declare liftPaal =
union
{
    cylinder {
        <0,-(KamerHoogte+2*WandDikte),0>
        <0,3*(KamerHoogte+WandDikte),0>
        liftPaalStraal
        pigment { color <1,1,1> }
        //material { Chroom }   // Expensive!!
    }
    object { liftPaalAnker translate -WandDikte/2*y }
    object { liftPaalAnker translate (KamerHoogte+WandDikte/2)*y }
}

#declare liftPalen = union {
    object { liftPaal rotate y*0    translate <LiftSchachtBreedte/2-(liftPaalStraal+liftPaalSpeling),0,LiftSchachtBreedte/2-(liftPaalStraal+liftPaalSpeling)> }
    object { liftPaal rotate y*270  translate <(liftPaalStraal+liftPaalSpeling)-LiftSchachtBreedte/2,0,LiftSchachtBreedte/2-(liftPaalStraal+liftPaalSpeling)> }
    object { liftPaal rotate y* 90 translate <LiftSchachtBreedte/2-(liftPaalStraal+liftPaalSpeling),0,(liftPaalStraal+liftPaalSpeling)-LiftSchachtBreedte/2> }
    object { liftPaal rotate y*180 translate <(liftPaalStraal+liftPaalSpeling)-LiftSchachtBreedte/2,0,(liftPaalStraal+liftPaalSpeling)-LiftSchachtBreedte/2> }
}

#declare KlauwStraal = 2*liftPaalStraal;
#declare KlauwBuisStraal = 0.6*liftPaalStraal;
#declare KlauwGatBreedte = 2*(LiftGap+liftPaalStraal+liftPaalSpeling);

#declare LiftKlauw = union {
    difference {
        cylinder { <0,-1.5*LiftDikte,0> <0,LiftDikte/2,0> KlauwStraal }
        box { <0,-(ActorsEps+1.5*LiftDikte),0> <KlauwStraal+ActorsEps,LiftDikte/2+ActorsEps,KlauwStraal+ActorsEps> }
    }
    cylinder { <0,-LiftDikte/2,0> <-2*KlauwGatBreedte,-LiftDikte/2,-2*KlauwGatBreedte> KlauwBuisStraal }
}

#declare lift = union {
    union {
         box {
            <LiftGap+KlauwGatBreedte-LiftSchachtBreedte/2,-LiftDikte,LiftGap-LiftSchachtBreedte/2>
            <LiftSchachtBreedte/2-(LiftGap+KlauwGatBreedte),0         ,LiftSchachtBreedte/2-LiftGap>
        }
         box {
            <LiftGap-LiftSchachtBreedte/2,-LiftDikte,LiftGap+KlauwGatBreedte-LiftSchachtBreedte/2>
            <LiftSchachtBreedte/2-LiftGap,0         ,LiftSchachtBreedte/2-(LiftGap+KlauwGatBreedte)>
        }
        object { LiftKlauw rotate y*0    translate <LiftSchachtBreedte/2-(liftPaalStraal+liftPaalSpeling),0,LiftSchachtBreedte/2-(liftPaalStraal+liftPaalSpeling)> }
        object { LiftKlauw rotate y*270  translate <(liftPaalStraal+liftPaalSpeling)-LiftSchachtBreedte/2,0,LiftSchachtBreedte/2-(liftPaalStraal+liftPaalSpeling)> }
        object { LiftKlauw rotate y* 90 translate <LiftSchachtBreedte/2-(liftPaalStraal+liftPaalSpeling),0,(liftPaalStraal+liftPaalSpeling)-LiftSchachtBreedte/2> }
        object { LiftKlauw rotate y*180 translate <(liftPaalStraal+liftPaalSpeling)-LiftSchachtBreedte/2,0,(liftPaalStraal+liftPaalSpeling)-LiftSchachtBreedte/2> }
        texture {
            pigment { KamerPigment }
        }
    }
}

object { liftPalen translate <KamerLengte/2,0,KamerDiepte/2> }
object { lift translate <KamerLengte/2,0.0*LiftDikte,KamerDiepte/2> }

#macro kratStapel1( n1 )
object { n1 }
#end

#macro kratStapel2( n1, n2 )
union {
    object { n1 rotate y*0 translate <0,0,0> }
    object { n2 rotate y*2 translate <0,kratHoogte,0> }
}
#end

#macro kratStapel3( n1, n2, n3 )
union {
    object { n1 rotate y*0 translate <0,0,0> }
    object { n2 rotate y*1 translate <0,kratHoogte,0> }
    object { n3 rotate y*-2 translate <0,2*(kratHoogte),0> }
}
#end

#macro kratStapel4( n1, n2, n3, n4 )
union {
    object { n1 rotate y*3 translate <0,0,0> }
    object { n2 rotate y*0 translate <0,kratHoogte,0> }
    object { n3 rotate y*-1 translate <0,2*(kratHoogte),0> }
    object { n4 rotate y*3 translate <0,3*(kratHoogte),0> }
}
#end


object { kratStapel1( krat ) translate <20,0,20> }
object { kratStapel1( krat ) translate <20,0,30> }
object { kratStapel1( krat ) translate <20,0,40> }

object { kratStapel4( cylinderFrame, krat, krat, VolleFruitKrat ) translate <20,0,80> }
object { kratStapel2( krat, krat ) translate <20,0,90> }
object { kratStapel1( krat ) translate <20,0,100> }

// ------------

object { kratStapel1( krat ) translate <30,0,20> }
object { kratStapel2( cylinderFrame, cylinderFrame ) translate <30,0,30> }
object { kratStapel2( krat, krat ) translate <30,0,40> }

object { kratStapel4( cylinderFrame, krat, krat, kratFrame ) translate <30,0,80> }
object { kratStapel2( krat, krat ) translate <30,0,90> }
object { kratStapel3( krat, krat, krat ) translate <30,0,100> }

// ------------

object { kratStapel2( krat, cylinderFrame ) translate <40,0,20> }
object { kratStapel4( krat, krat, bollenFrame, krat ) translate <40,0,30> }
object { kratStapel1( krat ) translate <40,0,40> }

object { kratStapel4( krat, krat, rollenFrame, krat ) translate <40,0,80> }
object { kratStapel1( krat ) translate <40,0,90> }
object { kratStapel3( krat, bollenFrame, krat ) translate <40,0,100> }

// ------------

object { kratStapel3( krat, krat, rollenFrame ) translate <50,0,20> }
object { kratStapel2( krat, krat ) translate <50,0,30> }
object { kratStapel1( krat ) translate <50,0,40> }

object { kratStapel1( bollenFrame ) translate <50,0,80> }
object { kratStapel3( krat, rollenFrame, krat ) translate <50,0,90> }
object { kratStapel2( krat, krat ) translate <50,0,100> }

// ------------

object { kratStapel4( krat, VolleFruitKrat, krat, krat ) translate <60,0,20> }
object { kratStapel4( krat, cylinderFrame, cylinderFrame, krat ) translate <60,0,30> }
object { kratStapel2( krat, krat ) translate <60,0,40> }

object { kratStapel3( krat, krat, krat ) translate <60,0,80> }
object { kratStapel3( krat, cylinderFrame, krat ) translate <60,0,90> }
object { kratStapel3( krat, krat, krat ) translate <60,0,100> }

// ------------

object { kratStapel3( krat, cylinderFrame, cylinderFrame ) translate <70,0,20> }
object { kratStapel3( krat, krat, kratFrame ) translate <70,0,30> }
object { kratStapel3( krat, krat, krat ) translate <70,0,40> }

object { kratStapel3( bollenFrame, cylinderFrame, krat ) translate <70,0,80> }
object { kratStapel3( cylinderFrame, krat, krat ) translate <70,0,90> }
object { kratStapel3( krat, bollenFrame, krat ) translate <70,0,100> }

// ------------

object { kratStapel3( krat, kratFrame, krat ) translate <80,0,20> }
object { kratStapel3( krat, krat, krat ) translate <80,0,30> }
object { kratStapel3( krat, cylinderFrame, krat ) translate <80,0,40> }

object { kratStapel3( krat, cylinderFrame, kratFrame ) translate <80,0,80> }
object { kratStapel3( rollenFrame, krat, krat ) translate <80,0,90> }
object { kratStapel3( krat, krat, krat ) translate <80,0,100> }

// ------------

object { kratStapel3( kratFrame, krat, rollenFrame ) translate <90,0,20> }
object { kratStapel2( krat, krat ) translate <90,0,30> }
object { kratStapel1( krat ) translate <90,0,40> }

object { kratStapel1( bollenFrame ) translate <90,0,80> }
object { kratStapel3( krat, rollenFrame, krat ) translate <90,0,90> }
object { kratStapel2( krat, krat ) translate <90,0,100> }


object { truck( lichtBruineVerf, Leeg )  translate <25,0,60> }

