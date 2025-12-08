// VERSION=3
function setup () {
    return {
        input : [
            {
                bands: ["B04", "B08", "CLM"],
                units: ["REFLECTANCE", "REFLECTANCE", "DN"]
            }
        ],
        output: {
            bands: 1,
            sampleType: "FLOAT32"
        }
    };
}
function evaluatePixel(sample) {
  if (sample.CLM == 1) {
    return [NaN];
  }
  let ndvi = index(sample.B08, sample.B04);
  return [ndvi];
}